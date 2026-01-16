"""
EU-22: QualityGateEvaluator

Capability: Evaluates data against quality threshold.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.22

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Evaluates quality gates only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Dataset read failure: Inconclusive
- Rules invalid: Rejected
- Evaluation timeout: Inconclusive
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    GateResult,
    QualityGateInput,
    QualityGateResult,
    QualityViolation,
    TenantContext,
)


class QualityRulesResolver(Protocol):
    """Protocol for quality rules resolution. Injected by Control Plane."""

    def resolve(
        self, rules_ref: str, tenant: TenantContext
    ) -> dict[str, Any]:
        """Resolve quality rules reference."""
        ...


class QualityEngine(Protocol):
    """Protocol for quality evaluation engine. Injected by Control Plane."""

    def evaluate(
        self,
        dataset_data: bytes,
        rules: dict[str, Any],
        thresholds: dict[str, float],
    ) -> tuple[bool, dict[str, float], list[dict[str, Any]]]:
        """
        Evaluate quality.
        Returns (passed, metric_values, violations).
        """
        ...


class DatasetReader(Protocol):
    """Protocol for dataset reading. Injected by Control Plane."""

    def read_dataset(
        self, dataset_ref: str, tenant: TenantContext
    ) -> bytes:
        """Read dataset data."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for QualityGateEvaluator."""
    gate_input: QualityGateInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for QualityGateEvaluator."""
    result: QualityGateResult | None
    error_code: str | None
    error_message: str | None
    is_inconclusive: bool


def execute(
    input_data: ExecutionInput,
    quality_rules_resolver: QualityRulesResolver,
    dataset_reader: DatasetReader,
    quality_engine: QualityEngine,
) -> ExecutionOutput:
    """
    Execute QualityGateEvaluator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    gate_input = input_data.gate_input

    # Resolve quality rules
    try:
        rules = quality_rules_resolver.resolve(
            gate_input.quality_rules_ref,
            input_data.tenant_context,
        )
    except RulesInvalidError as e:
        # Failure Mode: Rules invalid
        return ExecutionOutput(
            result=None,
            error_code="RULES_INVALID",
            error_message=f"Invalid quality rules: {e}",
            is_inconclusive=False,
        )

    # Read dataset
    try:
        dataset_data = dataset_reader.read_dataset(
            gate_input.dataset_ref,
            input_data.tenant_context,
        )
    except DatasetReadError as e:
        # Failure Mode: Dataset read failure - inconclusive
        return ExecutionOutput(
            result=None,
            error_code="DATASET_READ_FAILURE",
            error_message=f"Failed to read dataset: {e}",
            is_inconclusive=True,
        )

    # Evaluate quality
    try:
        passed, metric_values, raw_violations = quality_engine.evaluate(
            dataset_data=dataset_data,
            rules=rules,
            thresholds=gate_input.thresholds,
        )
    except EvaluationTimeoutError:
        # Failure Mode: Evaluation timeout - inconclusive
        return ExecutionOutput(
            result=None,
            error_code="EVALUATION_TIMEOUT",
            error_message="Quality evaluation timed out",
            is_inconclusive=True,
        )

    # Convert to QualityViolation objects
    violations = [
        QualityViolation(
            rule_name=v["rule_name"],
            expected=v["expected"],
            actual=v["actual"],
        )
        for v in raw_violations
    ]

    # Determine gate result
    gate_result = GateResult.PASS if passed else GateResult.FAIL

    return ExecutionOutput(
        result=QualityGateResult(
            result=gate_result,
            metric_values=metric_values,
            violations=violations,
            evaluated_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
        is_inconclusive=False,
    )


# Exception types for failure modes
class RulesInvalidError(Exception):
    """Quality rules are invalid."""


class DatasetReadError(Exception):
    """Failed to read dataset."""


class EvaluationTimeoutError(Exception):
    """Evaluation timed out."""
