"""
EU-20: LabelRecorder

Capability: Records single label annotation.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.20

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Records labels only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Task not found: Rejected
- Sample not in task: Rejected
- Schema violation: Rejected
- Storage failure: No annotation
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from ..schemas import (
    LabelRecordInput,
    LabelRecordResult,
    TenantContext,
)


class TaskRegistry(Protocol):
    """Protocol for task registry. Injected by Control Plane."""

    def get_task(
        self, task_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get task metadata."""
        ...


class LabelValidator(Protocol):
    """Protocol for label validation."""

    def validate_label(
        self,
        label_value: Any,
        schema_ref: str,
        tenant: TenantContext,
    ) -> tuple[bool, str | None]:
        """Validate label against schema. Returns (is_valid, error_message)."""
        ...


class AnnotationStore(Protocol):
    """Protocol for annotation storage. Injected by Control Plane."""

    def store_annotation(
        self,
        task_ref: str,
        sample_id: str,
        label_value: Any,
        annotator_ref: str,
        tenant: TenantContext,
    ) -> str:
        """Store annotation. Returns annotation identifier."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for LabelRecorder."""
    record_input: LabelRecordInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for LabelRecorder."""
    result: LabelRecordResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    task_registry: TaskRegistry,
    label_validator: LabelValidator,
    annotation_store: AnnotationStore,
) -> ExecutionOutput:
    """
    Execute LabelRecorder.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    record_input = input_data.record_input

    # Get task
    task = task_registry.get_task(
        record_input.task_ref,
        input_data.tenant_context,
    )
    if task is None:
        # Failure Mode: Task not found
        return ExecutionOutput(
            result=None,
            error_code="TASK_NOT_FOUND",
            error_message=f"Task not found: {record_input.task_ref}",
        )

    # Validate sample is in task
    sample_ids = task.get("sample_ids", [])
    if record_input.sample_id not in sample_ids:
        # Failure Mode: Sample not in task
        return ExecutionOutput(
            result=None,
            error_code="SAMPLE_NOT_IN_TASK",
            error_message=f"Sample not in task: {record_input.sample_id}",
        )

    # Validate label against schema
    schema_ref = task.get("schema_ref")
    is_valid, schema_error = label_validator.validate_label(
        label_value=record_input.label_value,
        schema_ref=schema_ref,
        tenant=input_data.tenant_context,
    )
    if not is_valid:
        # Failure Mode: Schema violation
        return ExecutionOutput(
            result=None,
            error_code="SCHEMA_VIOLATION",
            error_message=f"Label violates schema: {schema_error}",
        )

    # Store annotation
    try:
        annotation_id_str = annotation_store.store_annotation(
            task_ref=record_input.task_ref,
            sample_id=record_input.sample_id,
            label_value=record_input.label_value,
            annotator_ref=str(record_input.annotator_ref),
            tenant=input_data.tenant_context,
        )
    except StorageError as e:
        # Failure Mode: Storage failure
        return ExecutionOutput(
            result=None,
            error_code="STORAGE_FAILURE",
            error_message=f"Failed to store annotation: {e}",
        )

    return ExecutionOutput(
        result=LabelRecordResult(
            annotation_id=uuid4(),  # Use actual ID from store in production
            recorded_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class StorageError(Exception):
    """Storage operation failed."""
