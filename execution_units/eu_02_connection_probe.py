"""
EU-02: ConnectionProbe

Capability: Tests connectivity to external data source.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.2

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Probes connections only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Timeout: Returns unhealthy status
- Auth failure: Returns unhealthy with auth error
- Network unreachable: Returns unhealthy with network error
- Credential unavailable: Cannot start
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from ..schemas import (
    ConnectionProbeInput,
    ConnectionProbeResult,
    HealthStatus,
    TenantContext,
)


class ConnectionDriver(Protocol):
    """Protocol for connection driver. Injected by Control Plane."""

    def test_connection(
        self,
        connection_config: dict,
        credentials: dict,
        timeout_seconds: int,
    ) -> tuple[bool, int, str | None]:
        """Test connection. Returns (success, latency_ms, error)."""
        ...


class CredentialResolver(Protocol):
    """Protocol for credential resolution. Injected by Control Plane."""

    def resolve(self, credential_ref: str, tenant: TenantContext) -> dict:
        """Resolve credential reference to actual credentials."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for ConnectionProbe."""
    probe_input: ConnectionProbeInput
    connection_config: dict
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for ConnectionProbe."""
    result: ConnectionProbeResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    connection_driver: ConnectionDriver,
    credential_resolver: CredentialResolver,
) -> ExecutionOutput:
    """
    Execute ConnectionProbe.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    probe_input = input_data.probe_input

    # Resolve credentials
    try:
        credentials = credential_resolver.resolve(
            probe_input.credential_ref,
            input_data.tenant_context,
        )
    except CredentialUnavailableError:
        # Failure Mode: Credential unavailable - cannot start
        return ExecutionOutput(
            result=None,
            error_code="CREDENTIAL_UNAVAILABLE",
            error_message="Cannot resolve credential reference",
        )

    # Test connection
    try:
        success, latency_ms, error = connection_driver.test_connection(
            connection_config=input_data.connection_config,
            credentials=credentials,
            timeout_seconds=probe_input.timeout_seconds,
        )
    except TimeoutError:
        # Failure Mode: Timeout
        return ExecutionOutput(
            result=ConnectionProbeResult(
                health_status=HealthStatus.UNHEALTHY,
                latency_ms=probe_input.timeout_seconds * 1000,
                error_details="Connection timeout",
                probed_at=datetime.now(timezone.utc),
            ),
            error_code=None,
            error_message=None,
        )
    except AuthenticationError as e:
        # Failure Mode: Auth failure
        return ExecutionOutput(
            result=ConnectionProbeResult(
                health_status=HealthStatus.UNHEALTHY,
                latency_ms=0,
                error_details=f"Authentication failed: {e}",
                probed_at=datetime.now(timezone.utc),
            ),
            error_code=None,
            error_message=None,
        )
    except NetworkError as e:
        # Failure Mode: Network unreachable
        return ExecutionOutput(
            result=ConnectionProbeResult(
                health_status=HealthStatus.UNHEALTHY,
                latency_ms=0,
                error_details=f"Network error: {e}",
                probed_at=datetime.now(timezone.utc),
            ),
            error_code=None,
            error_message=None,
        )

    # Determine health status
    if success:
        health = HealthStatus.HEALTHY if latency_ms < 1000 else HealthStatus.DEGRADED
    else:
        health = HealthStatus.UNHEALTHY

    return ExecutionOutput(
        result=ConnectionProbeResult(
            health_status=health,
            latency_ms=latency_ms,
            error_details=error,
            probed_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class CredentialUnavailableError(Exception):
    """Credential cannot be resolved."""


class AuthenticationError(Exception):
    """Authentication failed."""


class NetworkError(Exception):
    """Network is unreachable."""
