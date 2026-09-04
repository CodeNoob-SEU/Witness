from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from react_agent.cost import (
    CostRecordKind,
    Price,
    PricingCatalog,
    UsageBreakdown,
    append_adjustment,
    summarize_costs,
)
from react_agent.telemetry import (
    GENAI_METRIC_NAMES,
    GENAI_SEMCONV_REVISION,
    MetricCardinalityPolicy,
    NoOpTelemetry,
    OTelTelemetry,
    RecordingTelemetry,
    TelemetryEvent,
    TelemetryEventKind,
    TelemetryMode,
    TraceReference,
    create_telemetry,
)

PRICING_TIME = datetime(2026, 1, 2, tzinfo=UTC)


def test_genai_development_semconv_names_are_a_frozen_golden_contract() -> None:
    assert GENAI_SEMCONV_REVISION == "development-snapshot-2026-08-20"
    assert dict(GENAI_METRIC_NAMES) == {
        "model_duration": "gen_ai.client.operation.duration",
        "token_usage": "gen_ai.client.token.usage",
        "agent_duration": "gen_ai.invoke_agent.operation.duration",
        "agent_count": "gen_ai.invoke_agent.operation.count",
        "tool_duration": "gen_ai.execute_tool.operation.duration",
        "tool_count": "gen_ai.execute_tool.operation.count",
    }


def test_catalog_quotes_detailed_usage_in_decimal_micro_units() -> None:
    usage = UsageBreakdown(
        input_tokens=1_000,
        output_tokens=500,
        cached_input_tokens=200,
        reasoning_output_tokens=100,
        billable_tokens=1_350,
    )
    catalog = PricingCatalog(
        "catalog-2026-01",
        (
            Price(
                provider="openai",
                model="gpt-test",
                version="price-v2",
                effective_from=datetime(2026, 1, 1, tzinfo=UTC),
                input_per_million=Decimal("2.50"),
                output_per_million=Decimal("10.00"),
                cached_input_per_million=Decimal("0.50"),
                reasoning_output_per_million=Decimal("12.00"),
            ),
        ),
    )

    record = catalog.quote(
        operation_id="model-1",
        provider="openai",
        model="gpt-test",
        response_model="gpt-test-2026-01-02",
        usage=usage,
        at=PRICING_TIME,
        record_id="cost-1",
    )

    assert record.is_known
    assert record.amount == Decimal("0.007300")
    assert record.amount_micros == 7_300
    assert record.operation_total == record.amount
    assert record.price_version == "price-v2"
    assert record.catalog_version == "catalog-2026-01"
    assert record.response_model == "gpt-test-2026-01-02"
    assert record.price_effective_from == datetime(2026, 1, 1, tzinfo=UTC)
    assert record.input_per_million == Decimal("2.50")
    assert record.output_per_million == Decimal("10.00")
    assert record.cached_input_per_million == Decimal("0.50")
    assert record.reasoning_output_per_million == Decimal("12.00")
    assert record.usage.billable_tokens == 1_350


def test_catalog_selects_effective_price_version() -> None:
    catalog = PricingCatalog(
        "catalog-versioned",
        (
            Price(
                "openai",
                "gpt-test",
                "old",
                datetime(2025, 1, 1, tzinfo=UTC),
                Decimal("1"),
                Decimal("2"),
                effective_to=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            Price(
                "openai",
                "gpt-test",
                "new",
                datetime(2026, 1, 1, tzinfo=UTC),
                Decimal("3"),
                Decimal("4"),
            ),
        ),
    )

    old_price = catalog.resolve(
        "OPENAI", "GPT-TEST", at=datetime(2025, 6, 1, tzinfo=UTC)
    )
    new_price = catalog.resolve("openai", "gpt-test", at=PRICING_TIME)
    assert old_price is not None
    assert new_price is not None
    assert old_price.version == "old"
    assert new_price.version == "new"


def test_unknown_cost_is_none_and_not_silently_zero() -> None:
    record = PricingCatalog("empty", ()).quote(
        operation_id="model-unknown",
        provider="compatible",
        model="private-model",
        usage=UsageBreakdown(input_tokens=10),
        at=PRICING_TIME,
        record_id="unknown-1",
    )

    assert not record.is_known
    assert record.amount is None
    assert record.amount_micros is None
    assert record.unknown_reason == "price_not_found"
    summary = summarize_costs((record,), currency="USD")
    assert summary.amount == Decimal("0.000000")
    assert summary.unresolved_records == 1


def test_adjustment_is_appended_and_preserves_history() -> None:
    catalog = PricingCatalog(
        "catalog",
        (
            Price(
                "openai",
                "gpt-test",
                "v1",
                datetime(2026, 1, 1, tzinfo=UTC),
                Decimal("1"),
                Decimal("1"),
            ),
        ),
    )
    original = catalog.quote(
        operation_id="model-1",
        provider="openai",
        model="gpt-test",
        usage=UsageBreakdown(input_tokens=1_000),
        at=PRICING_TIME,
        record_id="estimate-1",
    )
    history = (original,)

    adjusted = append_adjustment(
        history,
        previous_record_id="estimate-1",
        revised_total=Decimal("0.001400"),
        record_id="adjustment-1",
        at=PRICING_TIME,
        note="provider invoice reconciliation",
    )

    assert history == (original,)
    assert len(adjusted) == 2
    adjustment = adjusted[-1]
    assert adjustment.kind is CostRecordKind.ADJUSTMENT
    assert adjustment.adjusts_record_id == original.record_id
    assert adjustment.amount == Decimal("0.000400")
    assert adjustment.operation_total == Decimal("0.001400")
    assert summarize_costs(adjusted, currency="USD").amount == Decimal("0.001400")


def test_adjustment_can_resolve_an_unknown_estimate() -> None:
    unknown = PricingCatalog("empty", ()).quote(
        operation_id="model-1",
        provider="compatible",
        model="private-model",
        usage=UsageBreakdown(input_tokens=1),
        at=PRICING_TIME,
        record_id="unknown",
    )

    records = append_adjustment(
        (unknown,),
        previous_record_id="unknown",
        revised_total=Decimal("0.000123"),
        record_id="settled",
        at=PRICING_TIME,
    )

    summary = summarize_costs(records, currency="USD")
    assert summary.amount == Decimal("0.000123")
    assert summary.unresolved_records == 0


def test_price_rejects_float_money_and_usage_rejects_overlapping_details() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        Price(
            "openai",
            "gpt-test",
            "v1",
            PRICING_TIME,
            1.5,  # type: ignore[arg-type]
            Decimal("1"),
        )
    with pytest.raises(ValueError, match="cached_input_tokens"):
        UsageBreakdown(input_tokens=1, cached_input_tokens=2)


def test_recording_adapter_allowlists_attributes_and_suppresses_replay() -> None:
    adapter = RecordingTelemetry()
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_COMPLETED,
            {
                "run_id": "run-1",
                "operation_id": "model-1",
                "request_model": "gpt-test",
                "input_tokens": 5,
                "cached_input_tokens": 2,
                "reasoning_output_tokens": 1,
                "billable_tokens": 4,
                "ttfc_s": 0.01,
                "prompt": "must not be recorded",
                "arguments": "must not be recorded",
                "api_key": "must not be recorded",
                "reasoning": "must not be recorded",
                "reasoning_summary": "must not be recorded",
                "unknown_dynamic_key": "must not be recorded",
            },
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-1",
                "execution_id": "execution-2",
                "resume_reason": "process_restart",
            },
            mode=TelemetryMode.REPLAY,
        )
    )

    assert len(adapter.events) == 1
    attributes = adapter.events[0].attributes
    assert attributes == {
        "run_id": "run-1",
        "operation_id": "model-1",
        "request_model": "gpt-test",
        "input_tokens": 5,
        "cached_input_tokens": 2,
        "reasoning_output_tokens": 1,
        "billable_tokens": 4,
        "ttfc_s": 0.01,
    }


def test_missing_otel_dependency_falls_back_to_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(_name: str) -> Any:
        raise ImportError

    monkeypatch.setattr(
        "react_agent.telemetry.importlib.import_module",
        missing,
    )

    direct = OTelTelemetry()
    selected = create_telemetry()
    assert not direct.available
    assert isinstance(selected, NoOpTelemetry)
    direct.emit(TelemetryEvent(TelemetryEventKind.RUN_STARTED, {"run_id": "run-1"}))


class FakeInstrument:
    def __init__(self, name: str) -> None:
        self.name = name
        self.records: list[tuple[int | float, dict[str, Any]]] = []
        self.additions: list[tuple[int, dict[str, Any]]] = []

    def record(self, value: int | float, *, attributes: dict[str, Any]) -> None:
        self.records.append((value, dict(attributes)))

    def add(self, value: int, *, attributes: dict[str, Any]) -> None:
        self.additions.append((value, dict(attributes)))


class FakeMeter:
    def __init__(self) -> None:
        self.instruments: dict[str, FakeInstrument] = {}

    def _create(self, name: str) -> FakeInstrument:
        instrument = FakeInstrument(name)
        self.instruments[name] = instrument
        return instrument

    def create_histogram(self, name: str, **_kwargs: str) -> FakeInstrument:
        return self._create(name)

    def create_counter(self, name: str, **_kwargs: str) -> FakeInstrument:
        return self._create(name)


class FakeSpanContext:
    is_valid = True


class FakeSpan:
    def __init__(
        self,
        name: str,
        attributes: dict[str, Any],
        start_time: int | None,
        *,
        kind: Any | None = None,
        parent_context: Any | None = None,
        links: tuple[Any, ...] = (),
    ) -> None:
        self.name = name
        self.kind = kind
        self.attributes = dict(attributes)
        self.start_time = start_time
        self.parent_context = parent_context
        self.links = links
        self.span_context = FakeSpanContext()
        self.end_time: int | None = None
        self.events: list[str] = []

    def get_span_context(self) -> FakeSpanContext:
        return self.span_context

    def set_attribute(self, name: str, value: Any) -> None:
        self.attributes[name] = value

    def add_event(self, name: str, **_kwargs: Any) -> None:
        self.events.append(name)

    def end(self, *, end_time: int | None = None) -> None:
        self.end_time = end_time


class FakeTracer:
    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any],
        start_time: int | None = None,
        kind: Any | None = None,
        context: Any | None = None,
        links: tuple[Any, ...] = (),
    ) -> FakeSpan:
        span = FakeSpan(
            name,
            attributes,
            start_time,
            kind=kind,
            parent_context=context,
            links=links,
        )
        self.spans.append(span)
        return span


class ReferenceSpanContext:
    is_valid = True

    def __init__(self, trace_id: int, span_id: int, trace_flags: int = 1) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.trace_flags = trace_flags


class ReferenceSpan(FakeSpan):
    def __init__(self, *args: Any, trace_id: int, span_id: int, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.span_context = ReferenceSpanContext(trace_id, span_id)


class ReferenceTracer(FakeTracer):
    def __init__(self, trace_id: int) -> None:
        super().__init__()
        self.trace_id = trace_id

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any],
        start_time: int | None = None,
        kind: Any | None = None,
        context: Any | None = None,
        links: tuple[Any, ...] = (),
    ) -> FakeSpan:
        span = ReferenceSpan(
            name,
            attributes,
            start_time,
            kind=kind,
            parent_context=context,
            links=links,
            trace_id=self.trace_id,
            span_id=len(self.spans) + 1,
        )
        self.spans.append(span)
        return span


class FakeLogger:
    def __init__(self) -> None:
        self.records: list[Any] = []

    def emit(self, record: Any) -> None:
        self.records.append(record)


class FakeContextModule:
    class Context:
        def __init__(self, span: Any | None = None) -> None:
            self.span = span

    def __init__(self) -> None:
        self.cross_task_detaches: list[tuple[Any, Any]] = []

    def attach(self, context: Any) -> tuple[Any, Any]:
        return (asyncio.current_task(), context)

    def detach(self, token: tuple[Any, Any]) -> None:
        owner, _ = token
        current = asyncio.current_task()
        if owner is not current:
            self.cross_task_detaches.append((owner, current))
            raise ValueError("context token belongs to another task")


class FakeTraceModule:
    class SpanKind:
        INTERNAL = "INTERNAL"
        CLIENT = "CLIENT"

    @staticmethod
    def set_span_in_context(
        span: Any, context: FakeContextModule.Context | None = None
    ) -> FakeContextModule.Context:
        del context
        return FakeContextModule.Context(span)


def test_otel_lifecycle_correlates_domain_operations_across_journal_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace_module = FakeTraceModule()
    context_module = FakeContextModule()

    def fake_import(name: str) -> Any:
        if name == "opentelemetry.trace":
            return trace_module
        if name == "opentelemetry.context":
            return context_module
        raise ImportError(name)

    monkeypatch.setattr("react_agent.telemetry.importlib.import_module", fake_import)
    tracer = FakeTracer()
    adapter = OTelTelemetry(tracer=tracer, meter=FakeMeter(), logger=FakeLogger())
    common = {"run_id": "run-lifecycle", "execution_id": "execution-1"}

    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_STARTED,
            {**common, "agent_name": "react-agent"},
            timestamp_ns=1,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {
                **common,
                "operation_id": "model:s3:started:execution-1",
                "step": 3,
                "attempt": 2,
                "provider": "openai_compatible",
                "request_model": "gpt-5.6-terra",
            },
            timestamp_ns=2,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_COMPLETED,
            {
                **common,
                "operation_id": "model:s3:completed",
                "step": 3,
                "attempt": 2,
                "provider": "openai_compatible",
                "request_model": "gpt-5.6-terra",
                "outcome": "completed",
            },
            timestamp_ns=3,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.TOOL_STARTED,
            {
                **common,
                "operation_id": "tool:s3:t0:started:execution-1",
                "step": 3,
                "call_key": "s3:t0",
                "tool_name": "calculate",
            },
            timestamp_ns=4,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.TOOL_COMPLETED,
            {
                **common,
                "operation_id": "tool:s3:t0:completed",
                "step": 3,
                "call_key": "s3:t0",
                "tool_name": "calculate",
                "outcome": "completed",
            },
            timestamp_ns=5,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_COMPLETED,
            {**common, "status": "completed", "stop_reason": "completed"},
            timestamp_ns=6,
        )
    )

    assert [span.name for span in tracer.spans] == [
        "invoke_agent react-agent",
        "chat gpt-5.6-terra",
        "execute_tool calculate",
    ]
    assert [span.kind for span in tracer.spans] == [
        FakeTraceModule.SpanKind.INTERNAL,
        FakeTraceModule.SpanKind.CLIENT,
        FakeTraceModule.SpanKind.INTERNAL,
    ]
    assert [span.end_time for span in tracer.spans] == [6, 3, 5]
    root, model, tool = tracer.spans
    assert model.parent_context.span is root
    assert tool.parent_context.span is root


def test_otel_adapter_maps_spans_logs_and_bounded_genai_metrics() -> None:
    tracer = FakeTracer()
    meter = FakeMeter()
    logger = FakeLogger()
    adapter = OTelTelemetry(
        tracer=tracer,
        meter=meter,
        logger=logger,
        cardinality=MetricCardinalityPolicy(
            allowed_models=frozenset({"gpt-test"}),
            allowed_tools=frozenset({"calculate"}),
        ),
    )
    common = {"run_id": "run-high-cardinality", "execution_id": "exec-1"}

    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_STARTED,
            {**common, "agent_name": "react-agent"},
            timestamp_ns=1,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {
                **common,
                "operation_id": "model-1",
                "provider": "openai",
                "request_model": "gpt-test",
                "step": 1,
                "attempt": 1,
            },
            timestamp_ns=2,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_COMPLETED,
            {
                **common,
                "operation_id": "model-1",
                "provider": "openai",
                "request_model": "gpt-test",
                "response_model": "gpt-test-2026-01-01",
                "request_id": "request-high-cardinality",
                "step": 1,
                "attempt": 1,
                "outcome": "completed",
                "duration_s": 0.25,
                "ttfc_s": 0.04,
                "input_tokens": 10,
                "output_tokens": 4,
                "cached_input_tokens": 3,
                "reasoning_output_tokens": 2,
                "billable_tokens": 11,
                "cost_micros": 125,
                "currency": "USD",
                "content": "must not appear",
            },
            timestamp_ns=3,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.TOOL_STARTED,
            {
                **common,
                "operation_id": "tool-1",
                "tool_name": "calculate",
                "tool_call_id": "call-high-cardinality",
            },
            timestamp_ns=4,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.TOOL_COMPLETED,
            {
                **common,
                "operation_id": "tool-1",
                "tool_name": "calculate",
                "outcome": "completed",
                "duration_s": 0.01,
                "cached": False,
            },
            timestamp_ns=5,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.BUDGET_EXHAUSTED,
            {**common, "reason": "max_steps"},
            timestamp_ns=6,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_COMPLETED,
            {
                **common,
                "status": "completed",
                "stop_reason": "completed",
                "duration_s": 0.5,
            },
            timestamp_ns=7,
        )
    )

    assert [span.name for span in tracer.spans] == [
        "invoke_agent react-agent",
        "chat gpt-test",
        "execute_tool calculate",
    ]
    assert all(span.end_time is not None for span in tracer.spans)
    assert "budget_exhausted" in tracer.spans[0].events
    assert tracer.spans[1].attributes["gen_ai.response.id"] == "request-high-cardinality"
    assert "content" not in tracer.spans[1].attributes
    assert logger.records

    duration = meter.instruments["gen_ai.client.operation.duration"].records
    assert duration == [
        (
            0.25,
            {
                "gen_ai.operation.name": "chat",
                "gen_ai.provider.name": "openai",
                "gen_ai.request.model": "gpt-test",
                "react_agent.outcome": "completed",
            },
        )
    ]
    assert "run_id" not in duration[0][1]
    assert "request_id" not in duration[0][1]
    token_records = meter.instruments["gen_ai.client.token.usage"].records
    assert [record[0] for record in token_records] == [10, 4, 3, 2, 11]
    assert [record[1]["gen_ai.token.type"] for record in token_records] == [
        "input",
        "output",
        "cached_input",
        "reasoning_output",
        "billable",
    ]
    assert meter.instruments[
        "react_agent.gen_ai.client.time_to_first_content"
    ].records == [(0.04, duration[0][1])]
    assert (
        meter.instruments["gen_ai.execute_tool.operation.count"].additions[0][1][
            "gen_ai.tool.name"
        ]
        == "calculate"
    )

    forbidden_metric_keys = {
        "run_id",
        "execution_id",
        "operation_id",
        "request_id",
        "tool_call_id",
        "call_key",
    }
    for instrument in meter.instruments.values():
        metric_points = [*instrument.records, *instrument.additions]
        for _, metric_attributes in metric_points:
            assert forbidden_metric_keys.isdisjoint(metric_attributes)
            assert not any(
                key.endswith(".id") or key.endswith("_id")
                for key in metric_attributes
            )


@pytest.mark.asyncio
async def test_otel_async_lifecycle_uses_explicit_parent_without_cross_task_detach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace_module = FakeTraceModule()
    context_module = FakeContextModule()

    def fake_import(name: str) -> Any:
        if name == "opentelemetry.trace":
            return trace_module
        if name == "opentelemetry.context":
            return context_module
        raise ImportError(name)

    monkeypatch.setattr("react_agent.telemetry.importlib.import_module", fake_import)
    tracer = FakeTracer()
    adapter = OTelTelemetry(tracer=tracer, meter=FakeMeter(), logger=FakeLogger())
    common = {"run_id": "run-async", "execution_id": "execution-1"}

    async def submit_task() -> None:
        adapter.emit(
            TelemetryEvent(
                TelemetryEventKind.RUN_STARTED,
                {**common, "agent_name": "react-agent"},
                timestamp_ns=1,
            )
        )

    async def execution_task() -> None:
        adapter.emit(
            TelemetryEvent(
                TelemetryEventKind.MODEL_STARTED,
                {
                    **common,
                    "operation_id": "model-1",
                    "provider": "openai",
                    "request_model": "gpt-test",
                },
                timestamp_ns=2,
            )
        )
        adapter.emit(
            TelemetryEvent(
                TelemetryEventKind.MODEL_COMPLETED,
                {
                    **common,
                    "operation_id": "model-1",
                    "provider": "openai",
                    "request_model": "gpt-test",
                    "outcome": "completed",
                },
                timestamp_ns=3,
            )
        )
        adapter.emit(
            TelemetryEvent(
                TelemetryEventKind.RUN_COMPLETED,
                {**common, "status": "completed", "stop_reason": "completed"},
                timestamp_ns=4,
            )
        )

    await asyncio.create_task(submit_task())
    await asyncio.create_task(execution_task())

    root, model = tracer.spans
    assert isinstance(model.parent_context, FakeContextModule.Context)
    assert model.parent_context.span is root
    assert context_module.cross_task_detaches == []


def test_resume_starts_new_root_with_link_and_bounded_recovery_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace_module = FakeTraceModule()
    context_module = FakeContextModule()

    def fake_import(name: str) -> Any:
        if name == "opentelemetry.trace":
            return trace_module
        if name == "opentelemetry.context":
            return context_module
        raise ImportError(name)

    # Inject the trace/context seams instead of importing the real ones. The
    # OpenTelemetry SDK is an optional extra, so a test that reaches for the
    # ambient install passes or fails depending on how the venv was synced.
    monkeypatch.setattr("react_agent.telemetry.importlib.import_module", fake_import)
    tracer = FakeTracer()
    meter = FakeMeter()
    adapter = OTelTelemetry(tracer=tracer, meter=meter, logger=FakeLogger())

    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_STARTED,
            {
                "run_id": "run-1",
                "execution_id": "execution-1",
                "agent_name": "react-agent",
            },
            timestamp_ns=1,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {
                "run_id": "run-1",
                "execution_id": "execution-1",
                "operation_id": "model-1",
                "provider": "openai",
                "request_model": "gpt-test",
            },
            timestamp_ns=2,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-1",
                "execution_id": "execution-2",
                "previous_execution_id": "execution-1",
                "agent_name": "react-agent",
                "resume_reason": "process_restart",
                "prompt": "must not appear",
                "api_key": "must not appear",
            },
            timestamp_ns=3,
        )
    )

    assert len(tracer.spans) == 3
    first, interrupted_model, resumed = tracer.spans
    assert first.end_time == 3
    assert interrupted_model.end_time == 3
    assert first.attributes["react_agent.execution.outcome"] == "abandoned"
    assert interrupted_model.attributes["react_agent.execution.outcome"] == "abandoned"
    assert "execution_abandoned" in first.events
    assert resumed.attributes["react_agent.execution.kind"] == "resume"
    assert "prompt" not in resumed.attributes
    assert "api_key" not in resumed.attributes
    assert len(resumed.links) == 1
    link_context = getattr(resumed.links[0], "context", resumed.links[0])
    assert link_context is first.span_context
    assert resumed.parent_context is not None
    resume_points = meter.instruments["react_agent.run.resume.count"].additions
    assert resume_points == [
        (1, {"react_agent.resume.reason": "process_restart"})
    ]

    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-1",
                "execution_id": "execution-replay",
                "resume_reason": "process_restart",
            },
            timestamp_ns=4,
            mode=TelemetryMode.REPLAY,
        )
    )
    assert len(tracer.spans) == 3
    assert len(resume_points) == 1

    cross_process_tracer = FakeTracer()
    cross_process = OTelTelemetry(
        tracer=cross_process_tracer,
        meter=FakeMeter(),
        logger=FakeLogger(),
    )
    cross_process.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-1",
                "execution_id": "execution-remote",
                "resume_reason": "process_restart",
            },
            timestamp_ns=5,
        )
    )
    assert len(cross_process_tracer.spans) == 1
    assert cross_process_tracer.spans[0].links == ()


def test_trace_reference_is_a_strict_content_free_projection() -> None:
    reference = TraceReference(
        run_id="run-1",
        execution_id="execution-1",
        trace_id="1" * 32,
        span_id="2" * 16,
        trace_flags=1,
    )

    assert reference.trace_flags == 1
    with pytest.raises(ValueError, match="trace_id"):
        TraceReference("run-1", "execution-1", "0" * 32, "2" * 16)
    with pytest.raises(ValueError, match="span_id"):
        TraceReference("run-1", "execution-1", "1" * 32, "ABCDEF0123456789")
    with pytest.raises(ValueError, match="sampled bit"):
        TraceReference("run-1", "execution-1", "1" * 32, "2" * 16, 2)
    with pytest.raises(TypeError, match="TraceReference"):
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            links=(object(),),  # type: ignore[arg-type]
        )


def test_persisted_reference_links_a_cross_process_resume_to_a_new_trace() -> None:
    first_tracer = ReferenceTracer(trace_id=0x1234)
    first = OTelTelemetry(
        tracer=first_tracer,
        meter=FakeMeter(),
        logger=FakeLogger(),
    )
    reference = first.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_STARTED,
            {
                "run_id": "run-persistent-link",
                "execution_id": "execution-1",
                "agent_name": "react-agent",
            },
            timestamp_ns=1,
        )
    )

    assert reference == TraceReference(
        run_id="run-persistent-link",
        execution_id="execution-1",
        trace_id=f"{0x1234:032x}",
        span_id=f"{1:016x}",
        trace_flags=1,
    )

    resumed_tracer = ReferenceTracer(trace_id=0x5678)
    resumed = OTelTelemetry(
        tracer=resumed_tracer,
        meter=FakeMeter(),
        logger=FakeLogger(),
    )
    resumed_reference = resumed.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-persistent-link",
                "execution_id": "execution-2",
                "previous_execution_id": "execution-1",
                "agent_name": "react-agent",
                "resume_reason": "process_restart",
            },
            timestamp_ns=2,
            links=(reference,),
        )
    )

    assert resumed_reference is not None
    assert resumed_reference.trace_id == f"{0x5678:032x}"
    assert resumed_reference.trace_id != reference.trace_id
    assert len(resumed_tracer.spans[0].links) == 1
    linked_context = getattr(
        resumed_tracer.spans[0].links[0],
        "context",
        resumed_tracer.spans[0].links[0],
    )
    assert int(linked_context.trace_id) == int(reference.trace_id, 16)
    assert int(linked_context.span_id) == int(reference.span_id, 16)

    replayed = resumed.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_RESUMED,
            {
                "run_id": "run-persistent-link",
                "execution_id": "execution-replay",
                "previous_execution_id": "execution-2",
            },
            mode=TelemetryMode.REPLAY,
            links=(resumed_reference,),
        )
    )
    assert replayed is None
    assert len(resumed_tracer.spans) == 1


def test_otel_metrics_bucket_unapproved_dimensions_and_suppress_replay() -> None:
    tracer = FakeTracer()
    meter = FakeMeter()
    adapter = OTelTelemetry(tracer=tracer, meter=meter, logger=FakeLogger())
    event = TelemetryEvent(
        TelemetryEventKind.MODEL_COMPLETED,
        {
            "run_id": "run-1",
            "operation_id": "model-1",
            "provider": "tenant-specific-provider",
            "request_model": "tenant-model-1234",
            "outcome": "completed",
            "duration_s": 1.0,
        },
    )

    adapter.emit(event)
    attributes = meter.instruments["gen_ai.client.operation.duration"].records[0][1]
    assert attributes["gen_ai.provider.name"] == "other"
    assert attributes["gen_ai.request.model"] == "other"

    before = len(meter.instruments["gen_ai.client.operation.duration"].records)
    adapter.emit(
        TelemetryEvent(
            event.kind,
            event.attributes,
            mode=TelemetryMode.REPLAY,
        )
    )
    assert len(meter.instruments["gen_ai.client.operation.duration"].records) == before


def test_privacy_canary_never_reaches_span_log_or_metric_exports() -> None:
    canary = "OTEL_PRIVACY_CANARY_DO_NOT_EXPORT"
    tracer = FakeTracer()
    meter = FakeMeter()
    logger = FakeLogger()
    adapter = OTelTelemetry(
        tracer=tracer,
        meter=meter,
        logger=logger,
        cardinality=MetricCardinalityPolicy(
            allowed_models=frozenset({"gpt-test"}),
        ),
    )
    common = {"run_id": "run-canary", "execution_id": "execution-canary"}

    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_STARTED,
            {
                **common,
                "agent_name": "react-agent",
                "prompt": canary,
                "api_key": canary,
                "authorization": canary,
            },
            timestamp_ns=1,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {
                **common,
                "operation_id": "model-canary",
                "provider": "openai",
                "request_model": "gpt-test",
                "arguments": canary,
                "reasoning": canary,
            },
            timestamp_ns=2,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_COMPLETED,
            {
                **common,
                "operation_id": "model-canary",
                "provider": "openai",
                "request_model": "gpt-test",
                "outcome": "completed",
                "duration_s": 0.01,
                "input_tokens": 2,
                "output_tokens": 1,
                "result": canary,
                "raw": canary,
                "exception_message": canary,
            },
            timestamp_ns=3,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.RUN_COMPLETED,
            {**common, "status": "completed", "stop_reason": "completed"},
            timestamp_ns=4,
        )
    )

    span_export = [
        {"attributes": span.attributes, "events": span.events}
        for span in tracer.spans
    ]
    metric_export = {
        name: {"records": instrument.records, "additions": instrument.additions}
        for name, instrument in meter.instruments.items()
    }
    assert canary not in repr(span_export)
    assert canary not in repr(logger.records)
    assert canary not in repr(metric_export)


def test_otel_model_failure_spans_carry_the_retry_classification() -> None:
    tracer = FakeTracer()
    adapter = OTelTelemetry(
        tracer=tracer,
        meter=FakeMeter(),
        logger=FakeLogger(),
        cardinality=MetricCardinalityPolicy(allowed_models=frozenset({"gpt-test"})),
    )
    common = {"run_id": "run-1", "execution_id": "exec-1", "provider": "openai"}
    adapter.emit(TelemetryEvent(TelemetryEventKind.RUN_STARTED, common, timestamp_ns=1))
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {
                **common,
                "operation_id": "model-1",
                "request_model": "gpt-test",
                "step": 1,
                "attempt": 2,
            },
            timestamp_ns=2,
        )
    )
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_FAILED,
            {
                **common,
                "operation_id": "model-1",
                "request_model": "gpt-test",
                "step": 1,
                "attempt": 2,
                "error_type": "ModelInvocationError",
                "status_code": 429,
                "error_code": "rate_limit_exceeded",
                "retryable": True,
                "retry_exhausted": False,
                "execution_attempt": 2,
                "error": "provider prose must not appear",
                "error_param": "input[2].x",
            },
            timestamp_ns=3,
        )
    )

    span = next(span for span in tracer.spans if span.name.startswith("chat"))
    assert span.attributes["error.type"] == "ModelInvocationError"
    assert span.attributes["react_agent.model.status_code"] == 429
    assert span.attributes["react_agent.model.error_code"] == "rate_limit_exceeded"
    assert span.attributes["react_agent.model.retryable"] is True
    assert span.attributes["react_agent.model.retry_exhausted"] is False
    assert span.attributes["react_agent.model.execution_attempt"] == 2
    assert span.attributes["react_agent.attempt"] == 2
    assert not any("prose" in str(value) for value in span.attributes.values())
    assert not any("input[2]" in str(value) for value in span.attributes.values())


def test_child_spans_repeat_the_execution_kind_for_tail_sampling() -> None:
    tracer = FakeTracer()
    adapter = OTelTelemetry(tracer=tracer, meter=FakeMeter(), logger=FakeLogger())
    started = {"run_id": "run-1", "execution_id": "exec-1", "provider": "openai"}
    adapter.emit(TelemetryEvent(TelemetryEventKind.RUN_STARTED, started, timestamp_ns=1))
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.TOOL_STARTED,
            {**started, "operation_id": "tool-1", "tool_name": "probe", "call_key": "s1:t0"},
            timestamp_ns=2,
        )
    )
    resumed = {
        "run_id": "run-1",
        "execution_id": "exec-2",
        "previous_execution_id": "exec-1",
        "provider": "openai",
    }
    adapter.emit(TelemetryEvent(TelemetryEventKind.RUN_RESUMED, resumed, timestamp_ns=3))
    adapter.emit(
        TelemetryEvent(
            TelemetryEventKind.MODEL_STARTED,
            {**resumed, "operation_id": "model-2", "request_model": "gpt-test", "step": 2},
            timestamp_ns=4,
        )
    )

    assert [
        (span.name, span.attributes["react_agent.execution.kind"]) for span in tracer.spans
    ] == [
        ("invoke_agent", "start"),
        ("execute_tool probe", "start"),
        ("invoke_agent", "resume"),
        ("chat gpt-test", "resume"),
    ]
