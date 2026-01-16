"""
EU-13: SchemaValidator

Capability: Validates dataset against expected schema.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.13

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Validates schemas only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Dataset read failure: Inconclusive
- Schema unavailable: Cannot proceed
- Type inference failure: Inconclusive
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    SchemaDiscrepancy,
    SchemaValidationInput,
    SchemaValidationResult,
    TenantContext,
)


class SchemaResolver(Protocol):
    """Protocol for schema resolution. Injected by Control Plane."""

    def resolve(
        self, schema_ref: str, tenant: TenantContext
    ) -> dict[str, Any]:
        """Resolve schema reference to schema definition."""
        ...


class ValidationEngine(Protocol):
    """Protocol for validation engine. Injected by Control Plane."""

    def validate_schema(
        self,
        dataset_data: bytes,
        expected_schema: dict[str, Any],
        validation_mode: str,
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Validate schema. Returns (is_valid, discrepancies)."""
        ...


class DatasetReader(Protocol):
    """Protocol for dataset reading. Injected by Control Plane."""

    def read_dataset(
        self, dataset_ref: str, tenant: TenantContext
    ) -> bytes:
        """Read dataset data."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for SchemaValidator."""
    validation_input: SchemaValidationInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for SchemaValidator."""
    result: SchemaValidationResult | None
    error_code: str | None
    error_message: str | None
    is_inconclusive: bool


def execute(
    input_data: ExecutionInput,
    schema_resolver: SchemaResolver,
    dataset_reader: DatasetReader,
    validation_engine: ValidationEngine,
) -> ExecutionOutput:
    """
    Execute SchemaValidator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    val_input = input_data.validation_input

    # Resolve expected schema
    try:
        expected_schema = schema_resolver.resolve(
            val_input.expected_schema_ref,
            input_data.tenant_context,
        )
    except SchemaUnavailableError:
        # Failure Mode: Schema unavailable
        return ExecutionOutput(
            result=None,
            error_code="SCHEMA_UNAVAILABLE",
            error_message="Expected schema is unavailable",
            is_inconclusive=False,
        )

    # Read dataset
    try:
        dataset_data = dataset_reader.read_dataset(
            val_input.dataset_ref,
            input_data.tenant_context,
        )
    except DatasetReadError as e:
        # Failure Mode: Dataset read failure - inconclusive
        return ExecutionOutput(
            result=None,
            error_code="DATASET_READ_FAILURE",
            error_message=f"Failed to read dataset: {e}",
            is_inconclusive=True,
        )

    # Validate schema
    try:
        is_valid, raw_discrepancies = validation_engine.validate_schema(
            dataset_data=dataset_data,
            expected_schema=expected_schema,
            validation_mode=val_input.validation_mode.value,
        )
    except TypeInferenceError:
        # Failure Mode: Type inference failure - inconclusive
        return ExecutionOutput(
            result=None,
            error_code="TYPE_INFERENCE_FAILURE",
            error_message="Could not infer types from dataset",
            is_inconclusive=True,
        )

    # Convert to SchemaDiscrepancy objects
    discrepancies = [
        SchemaDiscrepancy(
            field_name=d["field_name"],
            expected_type=d["expected_type"],
            actual_type=d["actual_type"],
            issue=d["issue"],
        )
        for d in raw_discrepancies
    ]

    return ExecutionOutput(
        result=SchemaValidationResult(
            is_valid=is_valid,
            discrepancies=discrepancies,
            validated_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
        is_inconclusive=False,
    )


# Exception types for failure modes
class SchemaUnavailableError(Exception):
    """Schema is unavailable."""


class DatasetReadError(Exception):
    """Failed to read dataset."""


class TypeInferenceError(Exception):
    """Failed to infer types."""
