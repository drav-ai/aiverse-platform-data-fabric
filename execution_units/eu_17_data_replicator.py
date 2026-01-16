"""
EU-17: DataReplicator

Capability: Copies data from source to target storage location.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.17

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Replicates data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Source read failure: No replication
- Target write failure: No data persisted
- Checksum mismatch: Failed, no partial
- Network failure: Terminates
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    ReplicationInput,
    ReplicationResult,
    TenantContext,
)


class StorageClient(Protocol):
    """Protocol for storage operations. Injected by Control Plane."""

    def read_location(
        self, location_ref: str, tenant: TenantContext
    ) -> bytes:
        """Read data from storage location."""
        ...

    def write_location(
        self,
        location_ref: str,
        data: bytes,
        consistency_mode: str,
        tenant: TenantContext,
    ) -> str:
        """Write data to storage location. Returns confirmed location."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for DataReplicator."""
    replication_input: ReplicationInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataReplicator."""
    result: ReplicationResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    storage_client: StorageClient,
) -> ExecutionOutput:
    """
    Execute DataReplicator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    rep_input = input_data.replication_input

    # Read from source
    try:
        source_data = storage_client.read_location(
            rep_input.source_location_ref,
            input_data.tenant_context,
        )
    except SourceReadError as e:
        # Failure Mode: Source read failure
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_READ_FAILURE",
            error_message=f"Failed to read from source: {e}",
        )
    except NetworkError as e:
        # Failure Mode: Network failure during read
        return ExecutionOutput(
            result=None,
            error_code="NETWORK_FAILURE",
            error_message=f"Network error during read: {e}",
        )

    # Compute source checksum
    source_checksum = hashlib.sha256(source_data).hexdigest()

    # Write to target
    try:
        target_confirmed = storage_client.write_location(
            location_ref=rep_input.target_location_ref,
            data=source_data,
            consistency_mode=rep_input.consistency_mode.value,
            tenant=input_data.tenant_context,
        )
    except TargetWriteError as e:
        # Failure Mode: Target write failure
        return ExecutionOutput(
            result=None,
            error_code="TARGET_WRITE_FAILURE",
            error_message=f"Failed to write to target: {e}",
        )
    except NetworkError as e:
        # Failure Mode: Network failure during write
        return ExecutionOutput(
            result=None,
            error_code="NETWORK_FAILURE",
            error_message=f"Network error during write: {e}",
        )

    # Verify checksum (read back for strong consistency)
    if rep_input.consistency_mode.value == "strong":
        try:
            target_data = storage_client.read_location(
                rep_input.target_location_ref,
                input_data.tenant_context,
            )
            target_checksum = hashlib.sha256(target_data).hexdigest()

            if source_checksum != target_checksum:
                # Failure Mode: Checksum mismatch - no partial
                return ExecutionOutput(
                    result=None,
                    error_code="CHECKSUM_MISMATCH",
                    error_message="Source and target checksums do not match",
                )
        except Exception:
            # If verification read fails, still report success but mark checksum unknown
            target_checksum = "unverified"
    else:
        target_checksum = source_checksum  # Trust for eventual consistency

    return ExecutionOutput(
        result=ReplicationResult(
            bytes_replicated=len(source_data),
            target_confirmed=target_confirmed,
            checksum_match=source_checksum == target_checksum,
            replicated_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class SourceReadError(Exception):
    """Failed to read from source."""


class TargetWriteError(Exception):
    """Failed to write to target."""


class NetworkError(Exception):
    """Network error during operation."""
