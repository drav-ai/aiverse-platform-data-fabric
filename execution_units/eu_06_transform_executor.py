"""
EU-06: TransformExecutor

Capability: Applies single transformation to input, produces output.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.6

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Transforms data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Input read failure: No output
- Transform error: Rejected with details
- Output write failure: No output persisted
- Resource exhaustion: Terminates, no partial
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    TransformInput,
    TransformResult,
    TenantContext,
)


class TransformEngine(Protocol):
    """Protocol for transformation engine. Injected by Control Plane."""

    def apply_transform(
        self,
        input_data: bytes,
        transform_definition: dict[str, Any],
        parameters: dict[str, Any],
    ) -> tuple[bytes, int, int]:
        """Apply transformation. Returns (output_bytes, rows_in, rows_out)."""
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
    """Input contract for TransformExecutor."""
    transform_input: TransformInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for TransformExecutor."""
    result: TransformResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    transform_engine: TransformEngine,
    staging_io: StagingIO,
) -> ExecutionOutput:
    """
    Execute TransformExecutor.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    trans_input = input_data.transform_input

    # Read input data
    try:
        input_bytes = staging_io.read_staging(
            trans_input.input_data_ref,
            input_data.tenant_context,
        )
    except InputReadError as e:
        # Failure Mode: Input read failure
        return ExecutionOutput(
            result=None,
            error_code="INPUT_READ_FAILURE",
            error_message=f"Failed to read input: {e}",
        )

    # Apply transformation
    try:
        output_bytes, rows_in, rows_out = transform_engine.apply_transform(
            input_data=input_bytes,
            transform_definition=trans_input.transformation_definition,
            parameters=trans_input.parameters,
        )
    except TransformError as e:
        # Failure Mode: Transform error
        return ExecutionOutput(
            result=None,
            error_code="TRANSFORM_ERROR",
            error_message=f"Transformation failed: {e}",
        )
    except ResourceExhaustedError:
        # Failure Mode: Resource exhaustion - terminate cleanly
        return ExecutionOutput(
            result=None,
            error_code="RESOURCE_EXHAUSTED",
            error_message="Resource limits exceeded, terminated",
        )

    # Write output
    try:
        staging_io.write_staging(
            trans_input.output_staging_ref,
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

    # Compute transformation hash for lineage
    transform_hash = hashlib.sha256(
        str(trans_input.transformation_definition).encode()
        + str(trans_input.parameters).encode()
    ).hexdigest()[:16]

    return ExecutionOutput(
        result=TransformResult(
            rows_processed=rows_in,
            rows_output=rows_out,
            output_staging_ref=trans_input.output_staging_ref,
            transformation_hash=transform_hash,
            transformed_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class InputReadError(Exception):
    """Failed to read input data."""


class TransformError(Exception):
    """Transformation failed."""


class OutputWriteError(Exception):
    """Failed to write output."""


class ResourceExhaustedError(Exception):
    """Resource limits exceeded."""
