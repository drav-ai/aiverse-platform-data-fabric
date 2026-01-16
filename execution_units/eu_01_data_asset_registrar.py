"""
EU-01: DataAssetRegistrar

Capability: Writes data asset card to Registry.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.1

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Registers asset cards only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Registry unavailable: No registration occurs
- Invalid declaration: Rejected with validation error
- Duplicate conflict: Rejected with conflict indicator
- Authorization denied: Rejected with permission error
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID, uuid4

from ..schemas import (
    AssetDeclaration,
    RegistrationResult,
    TenantContext,
)


class RegistryClient(Protocol):
    """Protocol for Registry interaction. Injected by Control Plane."""

    def create_card(
        self,
        tenant: TenantContext,
        asset_type: str,
        name: str,
        version: str,
        metadata: dict,
    ) -> str:
        """Create asset card in registry. Returns card reference."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for DataAssetRegistrar."""
    asset_declaration: AssetDeclaration
    owner_ref: UUID
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataAssetRegistrar."""
    result: RegistrationResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    registry_client: RegistryClient,
) -> ExecutionOutput:
    """
    Execute DataAssetRegistrar.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    # Validate declaration
    declaration = input_data.asset_declaration
    if not declaration.name or not declaration.version:
        return ExecutionOutput(
            result=None,
            error_code="INVALID_DECLARATION",
            error_message="Asset name and version are required",
        )

    # Attempt registry write
    try:
        card_ref = registry_client.create_card(
            tenant=input_data.tenant_context,
            asset_type=declaration.asset_type.value,
            name=declaration.name,
            version=declaration.version,
            metadata={
                "schema": declaration.schema_declaration,
                "location": declaration.storage_location_ref,
                "classification": declaration.classification.value,
                "format": declaration.data_format.value,
                "owner": str(input_data.owner_ref),
            },
        )
    except RegistryUnavailableError:
        # Failure Mode: Registry unavailable
        return ExecutionOutput(
            result=None,
            error_code="REGISTRY_UNAVAILABLE",
            error_message="Registry service is unavailable",
        )
    except DuplicateAssetError:
        # Failure Mode: Duplicate conflict
        return ExecutionOutput(
            result=None,
            error_code="DUPLICATE_CONFLICT",
            error_message=f"Asset {declaration.name}:{declaration.version} already exists",
        )
    except AuthorizationError:
        # Failure Mode: Authorization denied
        return ExecutionOutput(
            result=None,
            error_code="AUTHORIZATION_DENIED",
            error_message="Not authorized to register assets in this namespace",
        )

    # Success
    return ExecutionOutput(
        result=RegistrationResult(
            asset_id=uuid4(),
            card_ref=card_ref,
            registered_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class RegistryUnavailableError(Exception):
    """Registry service is unavailable."""


class DuplicateAssetError(Exception):
    """Asset already exists."""


class AuthorizationError(Exception):
    """Not authorized."""
