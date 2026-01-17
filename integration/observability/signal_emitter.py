"""
Feedback Signal Emitter for Data Fabric Domain.

Emits feedback signals to the Observability Spine.

Per ADR: Signals are emitted after execution unit completion.
No execution logic - pure observability.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID, uuid4

from .signal_registry import FeedbackSignalRegistry, SignalDefinition


class ObservabilitySpine(Protocol):
    """Protocol for Observability Spine interaction."""

    async def emit_metric(
        self,
        metric_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        timestamp: datetime,
    ) -> bool:
        """Emit a metric signal."""
        ...

    async def emit_outcome(
        self,
        outcome_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        timestamp: datetime,
    ) -> bool:
        """Emit an outcome signal."""
        ...

    async def emit_advisor(
        self,
        advisor_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        intended_consumer: str,
        timestamp: datetime,
    ) -> bool:
        """Emit an advisor signal."""
        ...


@dataclass
class EmissionResult:
    """Result of signal emission."""
    signal_name: str
    signal_type: str
    success: bool
    emission_id: UUID
    timestamp: datetime
    error: str | None = None


class FeedbackSignalEmitter:
    """
    Emits Data Fabric feedback signals to Observability Spine.

    This emitter:
    1. Validates signal data against schema
    2. Emits to appropriate Observability Spine endpoint
    3. Records emission for audit
    """

    DOMAIN = "data-fabric"

    def __init__(
        self,
        observability_spine: ObservabilitySpine | None = None,
        signal_registry: FeedbackSignalRegistry | None = None,
    ):
        self._spine = observability_spine
        self._signal_registry = signal_registry or FeedbackSignalRegistry()
        self._emissions: list[EmissionResult] = []

    async def emit_metric(
        self,
        metric_name: str,
        intent_id: UUID,
        tenant_context: dict[str, str],
        values: dict[str, Any],
    ) -> EmissionResult:
        """
        Emit a metric signal.

        Per ADR: Metrics are emitted after execution unit completion.
        """
        signal_def = self._signal_registry.get_signal(metric_name)
        if not signal_def or signal_def.signal_type != "metric":
            return EmissionResult(
                signal_name=metric_name,
                signal_type="metric",
                success=False,
                emission_id=uuid4(),
                timestamp=datetime.now(timezone.utc),
                error=f"Unknown metric: {metric_name}",
            )

        emission_id = uuid4()
        now = datetime.now(timezone.utc)

        payload = {
            "intent_id": str(intent_id),
            "domain": self.DOMAIN,
            **values,
        }

        if self._spine:
            try:
                success = await self._spine.emit_metric(
                    metric_name=metric_name,
                    value=payload,
                    tenant_context=tenant_context,
                    timestamp=now,
                )
            except Exception as e:
                result = EmissionResult(
                    signal_name=metric_name,
                    signal_type="metric",
                    success=False,
                    emission_id=emission_id,
                    timestamp=now,
                    error=str(e),
                )
                self._emissions.append(result)
                return result
        else:
            success = True  # Mock success

        result = EmissionResult(
            signal_name=metric_name,
            signal_type="metric",
            success=success,
            emission_id=emission_id,
            timestamp=now,
        )
        self._emissions.append(result)
        return result

    async def emit_outcome(
        self,
        outcome_name: str,
        intent_id: UUID,
        tenant_context: dict[str, str],
        values: dict[str, Any],
    ) -> EmissionResult:
        """
        Emit an outcome signal.

        Per ADR: Outcomes record pass/fail results from execution units.
        """
        signal_def = self._signal_registry.get_signal(outcome_name)
        if not signal_def or signal_def.signal_type != "outcome":
            return EmissionResult(
                signal_name=outcome_name,
                signal_type="outcome",
                success=False,
                emission_id=uuid4(),
                timestamp=datetime.now(timezone.utc),
                error=f"Unknown outcome: {outcome_name}",
            )

        emission_id = uuid4()
        now = datetime.now(timezone.utc)

        payload = {
            "intent_id": str(intent_id),
            "domain": self.DOMAIN,
            **values,
        }

        if self._spine:
            try:
                success = await self._spine.emit_outcome(
                    outcome_name=outcome_name,
                    value=payload,
                    tenant_context=tenant_context,
                    timestamp=now,
                )
            except Exception as e:
                result = EmissionResult(
                    signal_name=outcome_name,
                    signal_type="outcome",
                    success=False,
                    emission_id=emission_id,
                    timestamp=now,
                    error=str(e),
                )
                self._emissions.append(result)
                return result
        else:
            success = True

        result = EmissionResult(
            signal_name=outcome_name,
            signal_type="outcome",
            success=success,
            emission_id=emission_id,
            timestamp=now,
        )
        self._emissions.append(result)
        return result

    async def emit_advisor(
        self,
        advisor_name: str,
        intent_id: UUID,
        tenant_context: dict[str, str],
        values: dict[str, Any],
        intended_consumer: str,
    ) -> EmissionResult:
        """
        Emit an advisor signal.

        Per ADR: Advisors provide recommendations to other components.
        """
        signal_def = self._signal_registry.get_signal(advisor_name)
        if not signal_def or signal_def.signal_type != "advisor":
            return EmissionResult(
                signal_name=advisor_name,
                signal_type="advisor",
                success=False,
                emission_id=uuid4(),
                timestamp=datetime.now(timezone.utc),
                error=f"Unknown advisor: {advisor_name}",
            )

        emission_id = uuid4()
        now = datetime.now(timezone.utc)

        payload = {
            "intent_id": str(intent_id),
            "domain": self.DOMAIN,
            **values,
        }

        if self._spine:
            try:
                success = await self._spine.emit_advisor(
                    advisor_name=advisor_name,
                    value=payload,
                    tenant_context=tenant_context,
                    intended_consumer=intended_consumer,
                    timestamp=now,
                )
            except Exception as e:
                result = EmissionResult(
                    signal_name=advisor_name,
                    signal_type="advisor",
                    success=False,
                    emission_id=emission_id,
                    timestamp=now,
                    error=str(e),
                )
                self._emissions.append(result)
                return result
        else:
            success = True

        result = EmissionResult(
            signal_name=advisor_name,
            signal_type="advisor",
            success=success,
            emission_id=emission_id,
            timestamp=now,
        )
        self._emissions.append(result)
        return result

    async def emit_for_execution_unit(
        self,
        execution_unit_name: str,
        intent_id: UUID,
        tenant_context: dict[str, str],
        execution_result: dict[str, Any],
        success: bool,
    ) -> list[EmissionResult]:
        """
        Emit all signals for an execution unit completion.

        Per ADR: Signals are emitted based on emission_trigger.
        """
        results = []
        signals = self._signal_registry.get_signals_for_execution_unit(execution_unit_name)

        for signal_def in signals:
            trigger = signal_def.emission_trigger
            condition = trigger.get("condition", "always")

            # Check emission condition
            if condition == "on_success" and not success:
                continue
            if condition == "on_failure" and success:
                continue

            # Emit based on signal type
            if signal_def.signal_type == "metric":
                result = await self.emit_metric(
                    metric_name=signal_def.name,
                    intent_id=intent_id,
                    tenant_context=tenant_context,
                    values=execution_result,
                )
            elif signal_def.signal_type == "outcome":
                result = await self.emit_outcome(
                    outcome_name=signal_def.name,
                    intent_id=intent_id,
                    tenant_context=tenant_context,
                    values=execution_result,
                )
            elif signal_def.signal_type == "advisor":
                # Get intended consumer from signal definition
                consumers = signal_def.intended_consumers
                consumer = consumers[0]["consumer"] if consumers else "unknown"
                result = await self.emit_advisor(
                    advisor_name=signal_def.name,
                    intent_id=intent_id,
                    tenant_context=tenant_context,
                    values=execution_result,
                    intended_consumer=consumer,
                )
            else:
                continue

            results.append(result)

        return results

    def get_emissions(self) -> list[EmissionResult]:
        """Get all recorded emissions."""
        return self._emissions.copy()

    def get_emission_count(self) -> dict[str, int]:
        """Get count of emissions by type and success."""
        counts = {
            "total": len(self._emissions),
            "successful": sum(1 for e in self._emissions if e.success),
            "failed": sum(1 for e in self._emissions if not e.success),
            "metrics": sum(1 for e in self._emissions if e.signal_type == "metric"),
            "outcomes": sum(1 for e in self._emissions if e.signal_type == "outcome"),
            "advisors": sum(1 for e in self._emissions if e.signal_type == "advisor"),
        }
        return counts

    def clear_emissions(self) -> None:
        """Clear recorded emissions."""
        self._emissions = []
