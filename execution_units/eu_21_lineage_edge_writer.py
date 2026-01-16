"""
EU-21: LineageEdgeWriter

Capability: Records lineage relationship between assets.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.21

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Records lineage edges only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Source not found: Rejected
- Target not found: Rejected
- Registry failure: No edge
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from ..schemas import (
    LineageEdgeInput,
    LineageEdgeResult,
    TenantContext,
)


class AssetRegistry(Protocol):
    """Protocol for asset registry. Injected by Control Plane."""

    def get_asset(
        self, asset_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get asset metadata."""
        ...


class LineageStore(Protocol):
    """Protocol for lineage storage. Injected by Control Plane."""

    def create_edge(
        self,
        source_ref: str,
        target_ref: str,
        relationship_type: str,
        execution_ref: str,
        tenant: TenantContext,
    ) -> str:
        """Create lineage edge. Returns edge identifier."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for LineageEdgeWriter."""
    edge_input: LineageEdgeInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for LineageEdgeWriter."""
    result: LineageEdgeResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    asset_registry: AssetRegistry,
    lineage_store: LineageStore,
) -> ExecutionOutput:
    """
    Execute LineageEdgeWriter.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    edge_input = input_data.edge_input

    # Validate source asset exists
    source_asset = asset_registry.get_asset(
        edge_input.source_asset_ref,
        input_data.tenant_context,
    )
    if source_asset is None:
        # Failure Mode: Source not found
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_NOT_FOUND",
            error_message=f"Source asset not found: {edge_input.source_asset_ref}",
        )

    # Validate target asset exists
    target_asset = asset_registry.get_asset(
        edge_input.target_asset_ref,
        input_data.tenant_context,
    )
    if target_asset is None:
        # Failure Mode: Target not found
        return ExecutionOutput(
            result=None,
            error_code="TARGET_NOT_FOUND",
            error_message=f"Target asset not found: {edge_input.target_asset_ref}",
        )

    # Create lineage edge
    try:
        edge_id_str = lineage_store.create_edge(
            source_ref=edge_input.source_asset_ref,
            target_ref=edge_input.target_asset_ref,
            relationship_type=edge_input.relationship_type,
            execution_ref=edge_input.execution_ref,
            tenant=input_data.tenant_context,
        )
    except RegistryError as e:
        # Failure Mode: Registry failure
        return ExecutionOutput(
            result=None,
            error_code="REGISTRY_FAILURE",
            error_message=f"Failed to create lineage edge: {e}",
        )

    return ExecutionOutput(
        result=LineageEdgeResult(
            edge_id=uuid4(),  # Use actual ID from store in production
            created_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class RegistryError(Exception):
    """Registry operation failed."""
