"""
EU-18: LocalitySignalGenerator

Capability: Produces locality signals for data asset.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.18

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Generates locality signals only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Asset not found: Rejected
- Location unreachable: Partial with indicator
- Timeout: Stale indicator
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    LocalityResult,
    LocalitySignal,
    LocalityType,
    TenantContext,
)


class AssetRegistry(Protocol):
    """Protocol for asset registry. Injected by Control Plane."""

    def get_asset(
        self, asset_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get asset metadata including storage locations."""
        ...


class LocalityProber(Protocol):
    """Protocol for locality probing. Injected by Control Plane."""

    def probe_locality(
        self,
        storage_locations: list[str],
        execution_environments: list[str],
    ) -> list[dict[str, Any]]:
        """
        Probe locality for each environment.
        Returns list of {environment_id, locality_type, transfer_cost, confidence}.
        """
        ...


class EnvironmentDiscovery(Protocol):
    """Protocol for discovering execution environments."""

    def get_environments(self, tenant: TenantContext) -> list[str]:
        """Get available execution environment IDs."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for LocalitySignalGenerator."""
    asset_ref: str
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for LocalitySignalGenerator."""
    result: LocalityResult | None
    error_code: str | None
    error_message: str | None
    has_stale_signals: bool


def execute(
    input_data: ExecutionInput,
    asset_registry: AssetRegistry,
    locality_prober: LocalityProber,
    environment_discovery: EnvironmentDiscovery,
) -> ExecutionOutput:
    """
    Execute LocalitySignalGenerator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    # Get asset metadata
    asset = asset_registry.get_asset(
        input_data.asset_ref,
        input_data.tenant_context,
    )
    if asset is None:
        # Failure Mode: Asset not found
        return ExecutionOutput(
            result=None,
            error_code="ASSET_NOT_FOUND",
            error_message=f"Asset not found: {input_data.asset_ref}",
            has_stale_signals=False,
        )

    # Get storage locations from asset
    storage_locations = asset.get("storage_locations", [])
    if not storage_locations:
        return ExecutionOutput(
            result=LocalityResult(
                signals=[],
                signal_freshness=datetime.now(timezone.utc),
            ),
            error_code=None,
            error_message=None,
            has_stale_signals=False,
        )

    # Get execution environments
    environments = environment_discovery.get_environments(
        input_data.tenant_context
    )

    # Probe locality
    try:
        raw_signals = locality_prober.probe_locality(
            storage_locations=storage_locations,
            execution_environments=environments,
        )
    except LocationUnreachableError as e:
        # Failure Mode: Location unreachable - return partial
        return ExecutionOutput(
            result=LocalityResult(
                signals=[
                    LocalitySignal(
                        environment_id=e.environment_id,
                        locality_type=LocalityType.UNAVAILABLE,
                        transfer_cost_estimate=-1.0,
                        confidence=0.0,
                    )
                ],
                signal_freshness=datetime.now(timezone.utc),
            ),
            error_code=None,
            error_message=f"Partial result: {e}",
            has_stale_signals=True,
        )
    except ProbeTimeoutError:
        # Failure Mode: Timeout - return stale indicator
        return ExecutionOutput(
            result=None,
            error_code="PROBE_TIMEOUT",
            error_message="Locality probe timed out",
            has_stale_signals=True,
        )

    # Convert to LocalitySignal objects
    signals = [
        LocalitySignal(
            environment_id=s["environment_id"],
            locality_type=LocalityType(s["locality_type"]),
            transfer_cost_estimate=s["transfer_cost"],
            confidence=s["confidence"],
        )
        for s in raw_signals
    ]

    # Check for stale signals (low confidence)
    has_stale = any(s.confidence < 0.5 for s in signals)

    return ExecutionOutput(
        result=LocalityResult(
            signals=signals,
            signal_freshness=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
        has_stale_signals=has_stale,
    )


# Exception types for failure modes
class LocationUnreachableError(Exception):
    """Location is unreachable."""

    def __init__(self, environment_id: str, message: str = ""):
        self.environment_id = environment_id
        super().__init__(message)


class ProbeTimeoutError(Exception):
    """Locality probe timed out."""
