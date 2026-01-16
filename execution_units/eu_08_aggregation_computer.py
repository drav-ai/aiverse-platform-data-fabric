"""
EU-08: AggregationComputer

Capability: Computes aggregates over grouped data.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.8

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Computes aggregations only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Input read failure: No output
- Invalid aggregation: Rejected
- Memory exhaustion: Terminates
- Output write failure: No output
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    AggregationInput,
    AggregationResult,
    TenantContext,
)


class AggregationEngine(Protocol):
    """Protocol for aggregation engine. Injected by Control Plane."""

    def compute_aggregates(
        self,
        input_data: bytes,
        group_by_columns: list[str],
        aggregations: dict[str, str],
    ) -> tuple[bytes, int]:
        """Compute aggregates. Returns (output_bytes, group_count)."""
        ...


class StagingIO(Protocol):
    """Protocol for staging I/O. Injected by Control Plane."""

    def read_staging(self, staging_ref: str, tenant: TenantContext) -> bytes:
        """Read from staging."""
        ...

    def write_staging(
        self, staging_ref: str, data: bytes, tenant: TenantContext
    ) -> None:
        """Write to staging."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for AggregationComputer."""
    aggregation_input: AggregationInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for AggregationComputer."""
    result: AggregationResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    aggregation_engine: AggregationEngine,
    staging_io: StagingIO,
) -> ExecutionOutput:
    """
    Execute AggregationComputer.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    agg_input = input_data.aggregation_input

    # Read input data
    try:
        input_bytes = staging_io.read_staging(
            agg_input.input_data_ref,
            input_data.tenant_context,
        )
    except InputReadError as e:
        # Failure Mode: Input read failure
        return ExecutionOutput(
            result=None,
            error_code="INPUT_READ_FAILURE",
            error_message=f"Failed to read input: {e}",
        )

    # Compute aggregates
    try:
        output_bytes, group_count = aggregation_engine.compute_aggregates(
            input_data=input_bytes,
            group_by_columns=agg_input.group_by_columns,
            aggregations=agg_input.aggregations,
        )
    except InvalidAggregationError as e:
        # Failure Mode: Invalid aggregation
        return ExecutionOutput(
            result=None,
            error_code="INVALID_AGGREGATION",
            error_message=f"Invalid aggregation: {e}",
        )
    except MemoryExhaustedError:
        # Failure Mode: Memory exhaustion - terminate cleanly
        return ExecutionOutput(
            result=None,
            error_code="MEMORY_EXHAUSTED",
            error_message="Memory limits exceeded, terminated",
        )

    # Write output
    try:
        staging_io.write_staging(
            agg_input.output_staging_ref,
            output_bytes,
            input_data.tenant_context,
        )
    except OutputWriteError as e:
        # Failure Mode: Output write failure
        return ExecutionOutput(
            result=None,
            error_code="OUTPUT_WRITE_FAILURE",
            error_message=f"Failed to write output: {e}",
        )

    return ExecutionOutput(
        result=AggregationResult(
            groups_computed=group_count,
            output_staging_ref=agg_input.output_staging_ref,
            aggregated_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class InputReadError(Exception):
    """Failed to read input data."""


class InvalidAggregationError(Exception):
    """Invalid aggregation specification."""


class MemoryExhaustedError(Exception):
    """Memory limits exceeded."""


class OutputWriteError(Exception):
    """Failed to write output."""
