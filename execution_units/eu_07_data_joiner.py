"""
EU-07: DataJoiner

Capability: Combines two inputs via join specification.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.7

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Joins data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Input read failure: No output
- Key mismatch: Rejected
- Memory exhaustion: Terminates
- Output write failure: No output
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    JoinInput,
    JoinResult,
    TenantContext,
)


class JoinEngine(Protocol):
    """Protocol for join engine. Injected by Control Plane."""

    def execute_join(
        self,
        left_data: bytes,
        right_data: bytes,
        join_keys: list[str],
        join_type: str,
    ) -> tuple[bytes, int, int, int, int]:
        """Execute join. Returns (output, rows, matched, unmatched_left, unmatched_right)."""
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
    """Input contract for DataJoiner."""
    join_input: JoinInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataJoiner."""
    result: JoinResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    join_engine: JoinEngine,
    staging_io: StagingIO,
) -> ExecutionOutput:
    """
    Execute DataJoiner.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    join_input = input_data.join_input

    # Read left input
    try:
        left_data = staging_io.read_staging(
            join_input.left_input_ref,
            input_data.tenant_context,
        )
    except InputReadError as e:
        # Failure Mode: Input read failure (left)
        return ExecutionOutput(
            result=None,
            error_code="LEFT_INPUT_READ_FAILURE",
            error_message=f"Failed to read left input: {e}",
        )

    # Read right input
    try:
        right_data = staging_io.read_staging(
            join_input.right_input_ref,
            input_data.tenant_context,
        )
    except InputReadError as e:
        # Failure Mode: Input read failure (right)
        return ExecutionOutput(
            result=None,
            error_code="RIGHT_INPUT_READ_FAILURE",
            error_message=f"Failed to read right input: {e}",
        )

    # Execute join
    try:
        output_bytes, rows_out, matched, unmatched_l, unmatched_r = (
            join_engine.execute_join(
                left_data=left_data,
                right_data=right_data,
                join_keys=join_input.join_keys,
                join_type=join_input.join_type.value,
            )
        )
    except KeyMismatchError as e:
        # Failure Mode: Key mismatch
        return ExecutionOutput(
            result=None,
            error_code="KEY_MISMATCH",
            error_message=f"Join key mismatch: {e}",
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
            join_input.output_staging_ref,
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
        result=JoinResult(
            rows_output=rows_out,
            matched_count=matched,
            unmatched_left=unmatched_l,
            unmatched_right=unmatched_r,
            output_staging_ref=join_input.output_staging_ref,
            joined_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class InputReadError(Exception):
    """Failed to read input data."""


class KeyMismatchError(Exception):
    """Join keys do not match between inputs."""


class MemoryExhaustedError(Exception):
    """Memory limits exceeded."""


class OutputWriteError(Exception):
    """Failed to write output."""
