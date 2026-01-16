"""
EU-04: DataExtractor

Capability: Reads data from source, writes to staging.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.4

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Extracts data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Source read failure: No data written
- Target write failure: No data persisted
- Quota exceeded: Not committed, bounds returned
- Format error: Rejected
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    DataExtractionInput,
    DataExtractionResult,
    TenantContext,
)


class DataReader(Protocol):
    """Protocol for data reading. Injected by Control Plane."""

    def read_data(
        self,
        connection_ref: str,
        query_or_path: str,
        offset: int,
        limit: int,
        tenant: TenantContext,
    ) -> tuple[bytes, int, str | None]:
        """Read data. Returns (data_bytes, row_count, watermark)."""
        ...


class StagingWriter(Protocol):
    """Protocol for staging writes. Injected by Control Plane."""

    def write_staging(
        self,
        staging_ref: str,
        data: bytes,
        output_format: str,
        tenant: TenantContext,
    ) -> int:
        """Write to staging. Returns bytes written."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for DataExtractor."""
    extraction_input: DataExtractionInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataExtractor."""
    result: DataExtractionResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    data_reader: DataReader,
    staging_writer: StagingWriter,
) -> ExecutionOutput:
    """
    Execute DataExtractor.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    ext_input = input_data.extraction_input

    # Read from source
    try:
        data_bytes, row_count, watermark = data_reader.read_data(
            connection_ref=ext_input.source_connection_ref,
            query_or_path=ext_input.source_query_or_path,
            offset=ext_input.extraction_offset,
            limit=ext_input.extraction_limit,
            tenant=input_data.tenant_context,
        )
    except SourceReadError as e:
        # Failure Mode: Source read failure
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_READ_FAILURE",
            error_message=f"Failed to read from source: {e}",
        )
    except FormatError as e:
        # Failure Mode: Format error
        return ExecutionOutput(
            result=None,
            error_code="FORMAT_ERROR",
            error_message=f"Data format error: {e}",
        )

    # Write to staging
    try:
        bytes_written = staging_writer.write_staging(
            staging_ref=ext_input.target_staging_ref,
            data=data_bytes,
            output_format=ext_input.output_format.value,
            tenant=input_data.tenant_context,
        )
    except StagingWriteError as e:
        # Failure Mode: Target write failure
        return ExecutionOutput(
            result=None,
            error_code="TARGET_WRITE_FAILURE",
            error_message=f"Failed to write to staging: {e}",
        )
    except QuotaExceededError:
        # Failure Mode: Quota exceeded
        return ExecutionOutput(
            result=None,
            error_code="QUOTA_EXCEEDED",
            error_message="Storage quota exceeded",
        )

    return ExecutionOutput(
        result=DataExtractionResult(
            bytes_extracted=bytes_written,
            rows_extracted=row_count,
            staging_ref=ext_input.target_staging_ref,
            watermark_value=watermark,
            extracted_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class SourceReadError(Exception):
    """Failed to read from source."""


class FormatError(Exception):
    """Data format error."""


class StagingWriteError(Exception):
    """Failed to write to staging."""


class QuotaExceededError(Exception):
    """Storage quota exceeded."""
