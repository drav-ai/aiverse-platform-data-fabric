"""
EU-09: FeatureComputer

Capability: Computes feature values for single feature definition.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.9

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Computes features only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Source read failure: No features
- Computation error: Rejected
- Entity key missing: Rejected
- Output write failure: No features persisted
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    FeatureComputeInput,
    FeatureComputeResult,
    TenantContext,
)


class FeatureEngine(Protocol):
    """Protocol for feature engine. Injected by Control Plane."""

    def compute_features(
        self,
        source_data: bytes,
        feature_definition: dict[str, Any],
        entity_key_columns: list[str],
        time_start: datetime,
        time_end: datetime,
    ) -> tuple[bytes, int, int]:
        """Compute features. Returns (output_bytes, entities, feature_count)."""
        ...


class FeatureDefinitionResolver(Protocol):
    """Protocol for resolving feature definitions."""

    def resolve(
        self, feature_def_ref: str, tenant: TenantContext
    ) -> dict[str, Any]:
        """Resolve feature definition reference."""
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
    """Input contract for FeatureComputer."""
    compute_input: FeatureComputeInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for FeatureComputer."""
    result: FeatureComputeResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    feature_engine: FeatureEngine,
    feature_def_resolver: FeatureDefinitionResolver,
    staging_io: StagingIO,
) -> ExecutionOutput:
    """
    Execute FeatureComputer.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    compute_input = input_data.compute_input

    # Resolve feature definition
    try:
        feature_definition = feature_def_resolver.resolve(
            compute_input.feature_definition_ref,
            input_data.tenant_context,
        )
    except DefinitionNotFoundError:
        return ExecutionOutput(
            result=None,
            error_code="DEFINITION_NOT_FOUND",
            error_message="Feature definition not found",
        )

    # Read source data
    try:
        source_data = staging_io.read_staging(
            compute_input.source_data_ref,
            input_data.tenant_context,
        )
    except SourceReadError as e:
        # Failure Mode: Source read failure
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_READ_FAILURE",
            error_message=f"Failed to read source: {e}",
        )

    # Compute features
    try:
        output_bytes, entities, feature_count = feature_engine.compute_features(
            source_data=source_data,
            feature_definition=feature_definition,
            entity_key_columns=compute_input.entity_key_columns,
            time_start=compute_input.time_start,
            time_end=compute_input.time_end,
        )
    except ComputationError as e:
        # Failure Mode: Computation error
        return ExecutionOutput(
            result=None,
            error_code="COMPUTATION_ERROR",
            error_message=f"Feature computation failed: {e}",
        )
    except EntityKeyMissingError as e:
        # Failure Mode: Entity key missing
        return ExecutionOutput(
            result=None,
            error_code="ENTITY_KEY_MISSING",
            error_message=f"Entity key column missing: {e}",
        )

    # Write output
    try:
        staging_io.write_staging(
            compute_input.output_staging_ref,
            output_bytes,
            input_data.tenant_context,
        )
    except OutputWriteError as e:
        # Failure Mode: Output write failure
        return ExecutionOutput(
            result=None,
            error_code="OUTPUT_WRITE_FAILURE",
            error_message=f"Failed to write features: {e}",
        )

    return ExecutionOutput(
        result=FeatureComputeResult(
            entities_computed=entities,
            feature_values_count=feature_count,
            output_staging_ref=compute_input.output_staging_ref,
            computed_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class DefinitionNotFoundError(Exception):
    """Feature definition not found."""


class SourceReadError(Exception):
    """Failed to read source data."""


class ComputationError(Exception):
    """Feature computation failed."""


class EntityKeyMissingError(Exception):
    """Entity key column is missing."""


class OutputWriteError(Exception):
    """Failed to write output."""
