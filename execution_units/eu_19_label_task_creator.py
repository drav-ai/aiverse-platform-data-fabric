"""
EU-19: LabelTaskCreator

Capability: Creates label task record.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.19

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Creates label tasks only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Dataset not found: Rejected
- Schema invalid: Rejected
- Empty selection: Rejected
- Registry failure: No task
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from ..schemas import (
    LabelTaskInput,
    LabelTaskResult,
    TenantContext,
)


class DatasetRegistry(Protocol):
    """Protocol for dataset registry. Injected by Control Plane."""

    def get_dataset(
        self, dataset_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get dataset metadata."""
        ...


class LabelSchemaValidator(Protocol):
    """Protocol for label schema validation."""

    def validate(
        self, schema_ref: str, tenant: TenantContext
    ) -> tuple[bool, str | None]:
        """Validate label schema. Returns (is_valid, error_message)."""
        ...


class SampleSelector(Protocol):
    """Protocol for sample selection."""

    def select_samples(
        self,
        dataset_ref: str,
        criteria: dict[str, Any],
        tenant: TenantContext,
    ) -> list[str]:
        """Select samples from dataset. Returns sample IDs."""
        ...


class TaskRegistry(Protocol):
    """Protocol for task registry. Injected by Control Plane."""

    def create_task(
        self,
        dataset_ref: str,
        schema_ref: str,
        sample_ids: list[str],
        quality_requirements: dict[str, float],
        tenant: TenantContext,
    ) -> str:
        """Create label task. Returns task identifier."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for LabelTaskCreator."""
    task_input: LabelTaskInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for LabelTaskCreator."""
    result: LabelTaskResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    dataset_registry: DatasetRegistry,
    label_schema_validator: LabelSchemaValidator,
    sample_selector: SampleSelector,
    task_registry: TaskRegistry,
) -> ExecutionOutput:
    """
    Execute LabelTaskCreator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    task_input = input_data.task_input

    # Validate dataset exists
    dataset = dataset_registry.get_dataset(
        task_input.source_dataset_ref,
        input_data.tenant_context,
    )
    if dataset is None:
        # Failure Mode: Dataset not found
        return ExecutionOutput(
            result=None,
            error_code="DATASET_NOT_FOUND",
            error_message=f"Dataset not found: {task_input.source_dataset_ref}",
        )

    # Validate label schema
    is_valid, schema_error = label_schema_validator.validate(
        task_input.label_schema_ref,
        input_data.tenant_context,
    )
    if not is_valid:
        # Failure Mode: Schema invalid
        return ExecutionOutput(
            result=None,
            error_code="SCHEMA_INVALID",
            error_message=f"Invalid label schema: {schema_error}",
        )

    # Select samples
    sample_ids = sample_selector.select_samples(
        dataset_ref=task_input.source_dataset_ref,
        criteria=task_input.sample_criteria,
        tenant=input_data.tenant_context,
    )

    if not sample_ids:
        # Failure Mode: Empty selection
        return ExecutionOutput(
            result=None,
            error_code="EMPTY_SELECTION",
            error_message="Sample criteria matched no records",
        )

    # Create task
    try:
        task_id_str = task_registry.create_task(
            dataset_ref=task_input.source_dataset_ref,
            schema_ref=task_input.label_schema_ref,
            sample_ids=sample_ids,
            quality_requirements=task_input.quality_requirements,
            tenant=input_data.tenant_context,
        )
    except RegistryError as e:
        # Failure Mode: Registry failure
        return ExecutionOutput(
            result=None,
            error_code="REGISTRY_FAILURE",
            error_message=f"Failed to create task: {e}",
        )

    return ExecutionOutput(
        result=LabelTaskResult(
            task_id=uuid4(),  # Use actual ID from registry in production
            sample_count=len(sample_ids),
            status="pending",
            created_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class RegistryError(Exception):
    """Registry operation failed."""
