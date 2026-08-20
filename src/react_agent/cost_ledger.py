"""Independent append-only ledger for post-hoc cost adjustments.

Run events deliberately stop at a terminal fact.  Provider invoices and
operator corrections can arrive later, so they live in this separate ledger
and are merged into :class:`~react_agent.events.RunSnapshot` only as a read
projection.  A cost adjustment therefore never consumes a durable run event
sequence or changes the run hash chain.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from .events import canonical_json

_HASH_DOMAIN = b"react-agent-cost-adjustment:v1\0"
_RECORD_ID_NAMESPACE = uuid.UUID("de83ce89-c339-4dcc-a997-f5ae61dd8758")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
MAX_COST_MICROS = 9_223_372_036_854_775_807
_INHERITED_PUBLIC_FIELDS = (
    "operation_id",
    "provider",
    "model",
    "request_model",
    "response_model",
    "catalog_version",
    "pricing_catalog_version",
    "price_version",
    "price_effective_from",
    "unit_prices_per_million",
    "usage",
)


class CostAdjustmentError(RuntimeError):
    """Base class for independent cost-ledger failures."""


class CostAdjustmentConflictError(CostAdjustmentError):
    """An immutable identity or predecessor was reused inconsistently."""


class CostRecordNotFoundError(CostAdjustmentError):
    """The requested predecessor is not part of the run's cost ledger."""


def _canonical_json(value: object) -> str:
    return canonical_json(value)


def _thaw(value: object) -> Any:
    return json.loads(_canonical_json(value))


def deterministic_adjustment_record_id(run_id: str, operation_id: str) -> str:
    """Bind one stable record id to a run-scoped idempotency operation."""

    if not run_id.strip() or not operation_id.strip():
        raise ValueError("run_id and operation_id must not be blank")
    return uuid.uuid5(_RECORD_ID_NAMESPACE, f"{run_id}\0{operation_id}").hex


@dataclass(frozen=True, slots=True)
class CostAdjustmentDraft:
    """Caller-controlled fields hashed for idempotent append."""

    record_id: str
    operation_id: str
    previous_record_id: str
    revised_total_micros: int
    note: str | None = None

    def __post_init__(self) -> None:
        for name in ("record_id", "operation_id", "previous_record_id"):
            value = getattr(self, name)
            if not value.strip():
                raise ValueError(f"{name} must not be blank")
            if len(value) > 256:
                raise ValueError(f"{name} must not exceed 256 characters")
        if self.record_id == self.previous_record_id:
            raise ValueError("an adjustment cannot reference itself")
        if (
            isinstance(self.revised_total_micros, bool)
            or not isinstance(self.revised_total_micros, int)
            or self.revised_total_micros < 0
            or self.revised_total_micros > MAX_COST_MICROS
        ):
            raise ValueError("revised_total_micros must be a non-negative signed 64-bit integer")
        if self.note is not None:
            if not isinstance(self.note, str):
                raise TypeError("note must be a string or None")
            if len(self.note) > 2_000:
                raise ValueError("note must not exceed 2000 characters")

    def payload_hash(self, run_id: str) -> str:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        body = _canonical_json(
            {
                "run_id": run_id,
                "record_id": self.record_id,
                "operation_id": self.operation_id,
                "previous_record_id": self.previous_record_id,
                "revised_total_micros": self.revised_total_micros,
                "note": self.note,
            }
        ).encode("utf-8")
        return hashlib.sha256(_HASH_DOMAIN + body).hexdigest()


@dataclass(frozen=True, slots=True)
class StoredCostAdjustment:
    """One committed, immutable adjustment record."""

    run_id: str
    ledger_sequence: int
    record_id: str
    operation_id: str
    previous_record_id: str
    payload_hash: str
    occurred_at: float
    public_payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be blank")
        if self.ledger_sequence < 1:
            raise ValueError("ledger_sequence must be positive")
        if len(self.payload_hash) != 64:
            raise ValueError("payload_hash must be a SHA-256 hex digest")
        if not math.isfinite(self.occurred_at):
            raise ValueError("occurred_at must be finite")
        object.__setattr__(self, "public_payload", MappingProxyType(_thaw(self.public_payload)))


@dataclass(frozen=True, slots=True)
class CostAdjustmentAppend:
    record: StoredCostAdjustment
    created: bool


@runtime_checkable
class CostAdjustmentStore(Protocol):
    """Persistence seam for adjustments that may outlive a terminal run."""

    async def append_cost_adjustment(
        self,
        run_id: str,
        draft: CostAdjustmentDraft,
        *,
        previous_record: Mapping[str, Any],
    ) -> CostAdjustmentAppend: ...

    async def list_cost_adjustments(
        self, run_id: str
    ) -> tuple[StoredCostAdjustment, ...]: ...


def _micros(value: object, *, field: str, allow_unknown: bool) -> int | None:
    if value is None and allow_unknown:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise CostAdjustmentConflictError(f"previous cost {field} is invalid")
    return value


def _previous_total(previous: Mapping[str, Any]) -> int | None:
    for field in (
        "operation_total_micros",
        "operation_total_microunits",
        "amount_micros",
        "amount_microunits",
    ):
        if field in previous:
            return _micros(previous[field], field=field, allow_unknown=True)
    raise CostAdjustmentConflictError("previous cost has no amount projection")


def build_cost_adjustment(
    run_id: str,
    draft: CostAdjustmentDraft,
    *,
    previous_record: Mapping[str, Any],
    ledger_sequence: int,
    occurred_at: float,
) -> StoredCostAdjustment:
    """Build the safe public projection from an immutable predecessor."""

    previous_id = previous_record.get("record_id")
    if previous_id != draft.previous_record_id:
        raise CostAdjustmentConflictError("previous cost identity changed before append")
    currency = previous_record.get("currency")
    if not isinstance(currency, str) or not _CURRENCY.fullmatch(currency):
        raise CostAdjustmentConflictError("previous cost currency is invalid")
    previous_total = _previous_total(previous_record)
    amount_micros = (
        draft.revised_total_micros
        if previous_total is None
        else draft.revised_total_micros - previous_total
    )
    public_payload: dict[str, Any] = {}
    for field in _INHERITED_PUBLIC_FIELDS:
        if field in previous_record:
            public_payload[field] = _thaw(previous_record[field])
    if "operation_id" in public_payload:
        public_payload["adjusted_operation_id"] = public_payload["operation_id"]
    public_payload.update(
        {
            "record_id": draft.record_id,
            "kind": "adjustment",
            "amount_micros": amount_micros,
            "operation_total_micros": draft.revised_total_micros,
            "revised_total_micros": draft.revised_total_micros,
            "currency": currency,
            "source": "manual_adjustment",
            "pricing_source": "manual_adjustment",
            "estimated": False,
            "adjusts_record_id": draft.previous_record_id,
            "adjustment_operation_id": draft.operation_id,
            "ledger_sequence": ledger_sequence,
            "priced_at": datetime.fromtimestamp(occurred_at, UTC).isoformat(),
            "unknown_reason": None,
            "note": draft.note,
        }
    )
    return StoredCostAdjustment(
        run_id=run_id,
        ledger_sequence=ledger_sequence,
        record_id=draft.record_id,
        operation_id=draft.operation_id,
        previous_record_id=draft.previous_record_id,
        payload_hash=draft.payload_hash(run_id),
        occurred_at=occurred_at,
        public_payload=public_payload,
    )


class InMemoryCostAdjustmentStore:
    """Concurrent reference adapter with two-key idempotency and linear chains."""

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._lock = asyncio.Lock()
        self._records: dict[str, list[StoredCostAdjustment]] = {}
        self._operations: dict[tuple[str, str], StoredCostAdjustment] = {}
        self._record_ids: dict[tuple[str, str], StoredCostAdjustment] = {}
        self._successors: dict[tuple[str, str], StoredCostAdjustment] = {}

    @staticmethod
    def _retry(
        existing: StoredCostAdjustment,
        *,
        payload_hash: str,
    ) -> CostAdjustmentAppend:
        if existing.payload_hash != payload_hash:
            raise CostAdjustmentConflictError(
                "cost adjustment identity was reused with different content"
            )
        return CostAdjustmentAppend(existing, created=False)

    async def append_cost_adjustment(
        self,
        run_id: str,
        draft: CostAdjustmentDraft,
        *,
        previous_record: Mapping[str, Any],
    ) -> CostAdjustmentAppend:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        payload_hash = draft.payload_hash(run_id)
        async with self._lock:
            by_operation = self._operations.get((run_id, draft.operation_id))
            if by_operation is not None:
                return self._retry(by_operation, payload_hash=payload_hash)
            by_record_id = self._record_ids.get((run_id, draft.record_id))
            if by_record_id is not None:
                return self._retry(by_record_id, payload_hash=payload_hash)
            successor = self._successors.get((run_id, draft.previous_record_id))
            if successor is not None:
                raise CostAdjustmentConflictError(
                    "previous cost already has an adjustment; reference the latest record"
                )
            previous_adjustment = self._record_ids.get(
                (run_id, draft.previous_record_id)
            )
            authoritative_previous = (
                previous_adjustment.public_payload
                if previous_adjustment is not None
                else previous_record
            )
            if authoritative_previous.get("record_id") != draft.previous_record_id:
                raise CostRecordNotFoundError(
                    f"cost record not found: {draft.previous_record_id}"
                )
            sequence = len(self._records.get(run_id, ())) + 1
            record = build_cost_adjustment(
                run_id,
                draft,
                previous_record=authoritative_previous,
                ledger_sequence=sequence,
                occurred_at=self._clock(),
            )
            self._records.setdefault(run_id, []).append(record)
            self._operations[(run_id, draft.operation_id)] = record
            self._record_ids[(run_id, draft.record_id)] = record
            self._successors[(run_id, draft.previous_record_id)] = record
            return CostAdjustmentAppend(record, created=True)

    async def list_cost_adjustments(
        self, run_id: str
    ) -> tuple[StoredCostAdjustment, ...]:
        if not run_id.strip():
            raise ValueError("run_id must not be blank")
        async with self._lock:
            return tuple(self._records.get(run_id, ()))
