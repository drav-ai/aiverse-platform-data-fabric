"""
EU-05: DataWriter

Capability: Writes staged data to target dataset.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.5

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Writes data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Staging read failure: No data written
- Target write failure: No data persisted
- Schema mismatch: Rejected
- Quota exceeded: Rejected
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    DataWriteInput,
    DataWriteResult,
    TenantContext,
)


class StagingReader(Protocol):
    """Protocol for staging reads. Injected by Control Plane."""

    def read_staging(
        self,
        staging_ref: str,
        tenant: TenantContext,
    ) -> tuple[bytes, dict[str, Any]]:
        """Read from staging. Returns (data_bytes, metadata)."""
        ...


class DatasetWriter(Protocol):
    """Protocol for dataset writes. Injected by Control Plane."""

    def write_dataset(
        self,
        dataset_ref: str,
        data: bytes,
        write_mode: str,
        partition_spec: dict[str, Any] | None,
        tenant: TenantContext,
    ) -> tuple[int, int, str]:
        """Write to dataset. Returns (bytes_written, rows_written, location)."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for DataWriter."""
    write_input: DataWriteInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataWriter."""
    result: DataWriteResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    staging_reader: StagingReader,
    dataset_writer: DatasetWriter,
) -> ExecutionOutput:
    """
    Execute DataWriter.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    write_input = input_data.write_input

    # Read from staging
    try:
        data_bytes, metadata = staging_reader.read_staging(
            staging_ref=write_input.staging_ref,
            tenant=input_data.tenant_context,
        )
    except StagingReadError as e:
        # Failure Mode: Staging read failure
        return ExecutionOutput(
            result=None,
            error_code="STAGING_READ_FAILURE",
            error_message=f"Failed to read from staging: {e}",
        )

    # Write to dataset
    try:
        bytes_written, rows_written, location = dataset_writer.write_dataset(
            dataset_ref=write_input.target_dataset_ref,
            data=data_bytes,
            write_mode=write_input.write_mode.value,
            partition_spec=write_input.partition_spec,
            tenant=input_data.tenant_context,
        )
    except TargetWriteError as e:
        # Failure Mode: Target write failure
        return ExecutionOutput(
            result=None,
            error_code="TARGET_WRITE_FAILURE",
            error_message=f"Failed to write to dataset: {e}",
        )
    except SchemaMismatchError as e:
        # Failure Mode: Schema mismatch
        return ExecutionOutput(
            result=None,
            error_code="SCHEMA_MISMATCH",
            error_message=f"Schema mismatch: {e}",
        )
    except QuotaExceededError:
        # Failure Mode: Quota exceeded
        return ExecutionOutput(
            result=None,
            error_code="QUOTA_EXCEEDED",
            error_message="Storage quota exceeded",
        )

    return ExecutionOutput(
        result=DataWriteResult(
            bytes_written=bytes_written,
            rows_written=rows_written,
            target_location=location,
            written_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class StagingReadError(Exception):
    """Failed to read from staging."""


class TargetWriteError(Exception):
    """Failed to write to target."""


class SchemaMismatchError(Exception):
    """Schema mismatch between staging and target."""


class QuotaExceededError(Exception):
    """Storage quota exceeded."""
