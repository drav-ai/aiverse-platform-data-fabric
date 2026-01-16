"""
EU-10: FeatureStoreWriter

Capability: Writes feature values to store (offline/online).

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.10

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Writes features to store only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Staging read failure: No features written
- Store write failure: No features persisted
- TTL invalid: Rejected
- Store unavailable: Cannot proceed
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    FeatureStoreWriteInput,
    FeatureStoreWriteResult,
    TenantContext,
)


class FeatureStoreClient(Protocol):
    """Protocol for feature store. Injected by Control Plane."""

    def write_features(
        self,
        feature_data: bytes,
        feature_set_ref: str,
        store_type: str,
        ttl_seconds: int,
        tenant: TenantContext,
    ) -> tuple[int, str]:
        """Write features. Returns (entities_written, store_location)."""
        ...


class StagingReader(Protocol):
    """Protocol for staging reads. Injected by Control Plane."""

    def read_staging(self, staging_ref: str, tenant: TenantContext) -> bytes:
        """Read from staging."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for FeatureStoreWriter."""
    write_input: FeatureStoreWriteInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for FeatureStoreWriter."""
    result: FeatureStoreWriteResult | None
    error_code: str | None
    error_message: str | None


MIN_TTL_SECONDS = 60
MAX_TTL_SECONDS = 31536000  # 1 year


def execute(
    input_data: ExecutionInput,
    staging_reader: StagingReader,
    feature_store_client: FeatureStoreClient,
) -> ExecutionOutput:
    """
    Execute FeatureStoreWriter.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    write_input = input_data.write_input

    # Validate TTL
    if write_input.ttl_seconds < MIN_TTL_SECONDS or write_input.ttl_seconds > MAX_TTL_SECONDS:
        # Failure Mode: TTL invalid
        return ExecutionOutput(
            result=None,
            error_code="TTL_INVALID",
            error_message=f"TTL must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS} seconds",
        )

    # Read from staging
    try:
        feature_data = staging_reader.read_staging(
            write_input.staging_ref,
            input_data.tenant_context,
        )
    except StagingReadError as e:
        # Failure Mode: Staging read failure
        return ExecutionOutput(
            result=None,
            error_code="STAGING_READ_FAILURE",
            error_message=f"Failed to read from staging: {e}",
        )

    # Write to feature store
    try:
        entities_written, store_location = feature_store_client.write_features(
            feature_data=feature_data,
            feature_set_ref=write_input.feature_set_ref,
            store_type=write_input.store_type.value,
            ttl_seconds=write_input.ttl_seconds,
            tenant=input_data.tenant_context,
        )
    except StoreWriteError as e:
        # Failure Mode: Store write failure
        return ExecutionOutput(
            result=None,
            error_code="STORE_WRITE_FAILURE",
            error_message=f"Failed to write to feature store: {e}",
        )
    except StoreUnavailableError:
        # Failure Mode: Store unavailable
        return ExecutionOutput(
            result=None,
            error_code="STORE_UNAVAILABLE",
            error_message="Feature store is unavailable",
        )

    return ExecutionOutput(
        result=FeatureStoreWriteResult(
            entities_written=entities_written,
            store_location=store_location,
            written_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class StagingReadError(Exception):
    """Failed to read from staging."""


class StoreWriteError(Exception):
    """Failed to write to feature store."""


class StoreUnavailableError(Exception):
    """Feature store is unavailable."""
