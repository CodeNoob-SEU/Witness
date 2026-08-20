"""Versioned, provider-neutral token cost accounting.

All monetary calculations use :class:`~decimal.Decimal` and are rounded to one
millionth of a currency unit.  A missing price remains explicitly unknown; it
is never silently converted to zero.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum

MICRO_UNIT = Decimal("0.000001")
TOKENS_PER_MILLION = Decimal(1_000_000)
_CURRENCY = re.compile(r"^[A-Z]{3}$")


def _require_decimal(name: str, value: Decimal) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")


def _require_non_negative_decimal(name: str, value: Decimal) -> None:
    _require_decimal(name, value)
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _micro(value: Decimal) -> Decimal:
    return value.quantize(MICRO_UNIT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True, slots=True)
class UsageBreakdown:
    """Billable token categories from one model operation.

    ``input_tokens`` includes cached input and ``output_tokens`` includes
    reasoning output.  The two detail fields identify the subsets that may use
    different prices.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_output_tokens: int = 0
    billable_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
            "reasoning_output_tokens",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.billable_tokens is not None and (
            isinstance(self.billable_tokens, bool)
            or not isinstance(self.billable_tokens, int)
            or self.billable_tokens < 0
        ):
            raise ValueError("billable_tokens must be a non-negative integer or None")
        if self.cached_input_tokens > self.input_tokens:
            raise ValueError("cached_input_tokens cannot exceed input_tokens")
        if self.reasoning_output_tokens > self.output_tokens:
            raise ValueError("reasoning_output_tokens cannot exceed output_tokens")

    @property
    def uncached_input_tokens(self) -> int:
        return self.input_tokens - self.cached_input_tokens

    @property
    def regular_output_tokens(self) -> int:
        return self.output_tokens - self.reasoning_output_tokens

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass(frozen=True, slots=True)
class Price:
    """One effective-dated model price, expressed per million tokens."""

    provider: str
    model: str
    version: str
    effective_from: datetime
    input_per_million: Decimal
    output_per_million: Decimal
    cached_input_per_million: Decimal | None = None
    reasoning_output_per_million: Decimal | None = None
    currency: str = "USD"
    effective_to: datetime | None = None

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.model.strip() or not self.version.strip():
            raise ValueError("provider, model, and price version must not be empty")
        _require_aware("effective_from", self.effective_from)
        if self.effective_to is not None:
            _require_aware("effective_to", self.effective_to)
            if self.effective_to <= self.effective_from:
                raise ValueError("effective_to must be later than effective_from")
        if not _CURRENCY.fullmatch(self.currency):
            raise ValueError("currency must be a three-letter uppercase code")
        _require_non_negative_decimal("input_per_million", self.input_per_million)
        _require_non_negative_decimal("output_per_million", self.output_per_million)
        if self.cached_input_per_million is not None:
            _require_non_negative_decimal("cached_input_per_million", self.cached_input_per_million)
        if self.reasoning_output_per_million is not None:
            _require_non_negative_decimal(
                "reasoning_output_per_million", self.reasoning_output_per_million
            )

    def applies_at(self, instant: datetime) -> bool:
        _require_aware("instant", instant)
        return self.effective_from <= instant and (
            self.effective_to is None or instant < self.effective_to
        )

    def calculate(self, usage: UsageBreakdown) -> Decimal:
        cached_rate = (
            self.cached_input_per_million
            if self.cached_input_per_million is not None
            else self.input_per_million
        )
        reasoning_rate = (
            self.reasoning_output_per_million
            if self.reasoning_output_per_million is not None
            else self.output_per_million
        )
        raw = (
            Decimal(usage.uncached_input_tokens) * self.input_per_million
            + Decimal(usage.cached_input_tokens) * cached_rate
            + Decimal(usage.regular_output_tokens) * self.output_per_million
            + Decimal(usage.reasoning_output_tokens) * reasoning_rate
        ) / TOKENS_PER_MILLION
        return _micro(raw)


class CostRecordKind(StrEnum):
    ESTIMATE = "estimate"
    ADJUSTMENT = "adjustment"


class CostSource(StrEnum):
    CATALOG_ESTIMATE = "catalog_estimate"
    PROVIDER_REPORTED = "provider_reported"
    MANUAL_ADJUSTMENT = "manual_adjustment"


@dataclass(frozen=True, slots=True)
class CostRecord:
    """One immutable contribution to an append-only cost ledger."""

    record_id: str
    operation_id: str
    kind: CostRecordKind
    amount: Decimal | None
    operation_total: Decimal | None
    currency: str
    provider: str
    model: str
    usage: UsageBreakdown
    source: CostSource
    catalog_version: str
    price_version: str | None
    priced_at: datetime
    response_model: str | None = None
    price_effective_from: datetime | None = None
    input_per_million: Decimal | None = None
    output_per_million: Decimal | None = None
    cached_input_per_million: Decimal | None = None
    reasoning_output_per_million: Decimal | None = None
    adjusts_record_id: str | None = None
    unknown_reason: str | None = None
    note: str | None = None

    def __post_init__(self) -> None:
        if not self.record_id.strip() or not self.operation_id.strip():
            raise ValueError("record_id and operation_id must not be empty")
        if not self.provider.strip() or not self.model.strip():
            raise ValueError("provider and model must not be empty")
        if not self.catalog_version.strip():
            raise ValueError("catalog_version must not be empty")
        if not _CURRENCY.fullmatch(self.currency):
            raise ValueError("currency must be a three-letter uppercase code")
        _require_aware("priced_at", self.priced_at)
        if self.response_model is not None and not self.response_model.strip():
            raise ValueError("response_model must be non-empty or None")
        if self.price_effective_from is not None:
            _require_aware("price_effective_from", self.price_effective_from)
        rates = (
            self.input_per_million,
            self.output_per_million,
            self.cached_input_per_million,
            self.reasoning_output_per_million,
        )
        for name, rate in zip(
            (
                "input_per_million",
                "output_per_million",
                "cached_input_per_million",
                "reasoning_output_per_million",
            ),
            rates,
            strict=True,
        ):
            if rate is not None:
                _require_non_negative_decimal(name, rate)
        if self.price_version is None:
            if self.price_effective_from is not None or any(rate is not None for rate in rates):
                raise ValueError("unknown prices cannot claim frozen unit rates")
        elif (
            self.price_effective_from is None
            or self.input_per_million is None
            or self.output_per_million is None
        ):
            raise ValueError("priced records require effective time and input/output rates")
        if self.amount is None or self.operation_total is None:
            if self.amount is not None or self.operation_total is not None:
                raise ValueError("amount and operation_total must both be known or unknown")
            if self.kind is not CostRecordKind.ESTIMATE or not self.unknown_reason:
                raise ValueError("only an estimate with a reason may have unknown cost")
        else:
            _require_decimal("amount", self.amount)
            _require_non_negative_decimal("operation_total", self.operation_total)
            if self.kind is CostRecordKind.ESTIMATE and self.amount < 0:
                raise ValueError("an estimated amount cannot be negative")
            if self.amount != _micro(self.amount) or self.operation_total != _micro(
                self.operation_total
            ):
                raise ValueError("cost values must be rounded to micro units")
        if self.kind is CostRecordKind.ADJUSTMENT and not self.adjusts_record_id:
            raise ValueError("an adjustment must reference the previous record")
        if self.kind is CostRecordKind.ESTIMATE and self.adjusts_record_id is not None:
            raise ValueError("an estimate cannot reference an adjusted record")

    @property
    def is_known(self) -> bool:
        return self.amount is not None

    @property
    def amount_micros(self) -> int | None:
        if self.amount is None:
            return None
        return int(self.amount / MICRO_UNIT)


@dataclass(frozen=True, slots=True)
class CostSummary:
    currency: str
    amount: Decimal
    unresolved_records: int

    @property
    def amount_micros(self) -> int:
        return int(self.amount / MICRO_UNIT)


@dataclass(frozen=True, slots=True)
class PricingCatalog:
    """Versioned price lookup and quote module."""

    version: str
    prices: tuple[Price, ...]
    default_currency: str = "USD"

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("catalog version must not be empty")
        if not _CURRENCY.fullmatch(self.default_currency):
            raise ValueError("default_currency must be a three-letter uppercase code")
        object.__setattr__(self, "prices", tuple(self.prices))
        identities: set[tuple[str, str, datetime]] = set()
        for price in self.prices:
            identity = (
                price.provider.casefold(),
                price.model.casefold(),
                price.effective_from,
            )
            if identity in identities:
                raise ValueError("duplicate effective price for provider and model")
            identities.add(identity)

    def resolve(self, provider: str, model: str, *, at: datetime) -> Price | None:
        _require_aware("at", at)
        candidates = (
            price
            for price in self.prices
            if price.provider.casefold() == provider.casefold()
            and price.model.casefold() == model.casefold()
            and price.applies_at(at)
        )
        return max(candidates, key=lambda price: price.effective_from, default=None)

    def quote(
        self,
        *,
        operation_id: str,
        provider: str,
        model: str,
        response_model: str | None = None,
        usage: UsageBreakdown,
        at: datetime | None = None,
        record_id: str | None = None,
    ) -> CostRecord:
        priced_at = at or datetime.now(UTC)
        _require_aware("at", priced_at)
        resolved = self.resolve(provider, model, at=priced_at)
        resolved_record_id = record_id or uuid.uuid4().hex
        if resolved is None:
            return CostRecord(
                record_id=resolved_record_id,
                operation_id=operation_id,
                kind=CostRecordKind.ESTIMATE,
                amount=None,
                operation_total=None,
                currency=self.default_currency,
                provider=provider,
                model=model,
                response_model=response_model,
                usage=usage,
                source=CostSource.CATALOG_ESTIMATE,
                catalog_version=self.version,
                price_version=None,
                priced_at=priced_at,
                unknown_reason="price_not_found",
            )
        amount = resolved.calculate(usage)
        return CostRecord(
            record_id=resolved_record_id,
            operation_id=operation_id,
            kind=CostRecordKind.ESTIMATE,
            amount=amount,
            operation_total=amount,
            currency=resolved.currency,
            provider=provider,
            model=model,
            response_model=response_model,
            usage=usage,
            source=CostSource.CATALOG_ESTIMATE,
            catalog_version=self.version,
            price_version=resolved.version,
            priced_at=priced_at,
            price_effective_from=resolved.effective_from,
            input_per_million=resolved.input_per_million,
            output_per_million=resolved.output_per_million,
            cached_input_per_million=(
                resolved.cached_input_per_million
                if resolved.cached_input_per_million is not None
                else resolved.input_per_million
            ),
            reasoning_output_per_million=(
                resolved.reasoning_output_per_million
                if resolved.reasoning_output_per_million is not None
                else resolved.output_per_million
            ),
        )


def append_adjustment(
    records: Sequence[CostRecord],
    *,
    previous_record_id: str,
    revised_total: Decimal,
    source: CostSource = CostSource.PROVIDER_REPORTED,
    record_id: str | None = None,
    at: datetime | None = None,
    note: str | None = None,
) -> tuple[CostRecord, ...]:
    """Return a new ledger with an adjustment appended; never mutate history."""

    _require_non_negative_decimal("revised_total", revised_total)
    previous = next(
        (record for record in reversed(records) if record.record_id == previous_record_id),
        None,
    )
    if previous is None:
        raise ValueError("previous_record_id was not found")
    current_total = previous.operation_total
    rounded_total = _micro(revised_total)
    delta = rounded_total if current_total is None else _micro(rounded_total - current_total)
    adjustment = CostRecord(
        record_id=record_id or uuid.uuid4().hex,
        operation_id=previous.operation_id,
        kind=CostRecordKind.ADJUSTMENT,
        amount=delta,
        operation_total=rounded_total,
        currency=previous.currency,
        provider=previous.provider,
        model=previous.model,
        response_model=previous.response_model,
        usage=previous.usage,
        source=source,
        catalog_version=previous.catalog_version,
        price_version=previous.price_version,
        priced_at=at or datetime.now(UTC),
        price_effective_from=previous.price_effective_from,
        input_per_million=previous.input_per_million,
        output_per_million=previous.output_per_million,
        cached_input_per_million=previous.cached_input_per_million,
        reasoning_output_per_million=previous.reasoning_output_per_million,
        adjusts_record_id=previous.record_id,
        note=note,
    )
    return (*records, adjustment)


def summarize_costs(records: Sequence[CostRecord], *, currency: str) -> CostSummary:
    """Sum immutable ledger contributions while preserving unresolved count."""

    if not _CURRENCY.fullmatch(currency):
        raise ValueError("currency must be a three-letter uppercase code")
    relevant = tuple(record for record in records if record.currency == currency)
    resolved_unknown_ids = {
        record.adjusts_record_id
        for record in relevant
        if record.kind is CostRecordKind.ADJUSTMENT and record.operation_total is not None
    }
    unresolved = sum(
        record.amount is None and record.record_id not in resolved_unknown_ids
        for record in relevant
    )
    total = sum(
        (record.amount for record in relevant if record.amount is not None),
        start=Decimal(0),
    )
    return CostSummary(currency, _micro(total), unresolved)
