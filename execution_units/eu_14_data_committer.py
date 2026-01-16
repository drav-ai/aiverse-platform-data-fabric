"""
EU-14: DataCommitter

Capability: Creates immutable commit record for dataset state.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.14

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Creates commits only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Dataset read failure: No commit
- Parent not found: Rejected
- Commit storage failure: No commit persisted
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    CommitInput,
    CommitResult,
    TenantContext,
)


class CommitStore(Protocol):
    """Protocol for commit storage. Injected by Control Plane."""

    def create_commit(
        self,
        dataset_ref: str,
        parent_ref: str | None,
        content_hash: str,
        changeset: dict[str, int],
        message: str,
        author: str,
        tenant: TenantContext,
    ) -> str:
        """Create commit. Returns commit identifier."""
        ...

    def get_commit(
        self, commit_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get commit by reference."""
        ...


class DatasetReader(Protocol):
    """Protocol for dataset reading. Injected by Control Plane."""

    def read_dataset_state(
        self, dataset_ref: str, tenant: TenantContext
    ) -> tuple[bytes, dict[str, int]]:
        """Read dataset state. Returns (content, changeset_from_parent)."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for DataCommitter."""
    commit_input: CommitInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataCommitter."""
    result: CommitResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    dataset_reader: DatasetReader,
    commit_store: CommitStore,
) -> ExecutionOutput:
    """
    Execute DataCommitter.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    commit_input = input_data.commit_input

    # Validate parent commit exists if specified
    if commit_input.parent_commit_ref:
        parent = commit_store.get_commit(
            commit_input.parent_commit_ref,
            input_data.tenant_context,
        )
        if parent is None:
            # Failure Mode: Parent not found
            return ExecutionOutput(
                result=None,
                error_code="PARENT_NOT_FOUND",
                error_message=f"Parent commit not found: {commit_input.parent_commit_ref}",
            )

    # Read current dataset state
    try:
        content, changeset = dataset_reader.read_dataset_state(
            commit_input.dataset_ref,
            input_data.tenant_context,
        )
    except DatasetReadError as e:
        # Failure Mode: Dataset read failure
        return ExecutionOutput(
            result=None,
            error_code="DATASET_READ_FAILURE",
            error_message=f"Failed to read dataset state: {e}",
        )

    # Compute content hash for immutability
    content_hash = hashlib.sha256(content).hexdigest()

    # Create commit
    try:
        commit_id = commit_store.create_commit(
            dataset_ref=commit_input.dataset_ref,
            parent_ref=commit_input.parent_commit_ref,
            content_hash=content_hash,
            changeset=changeset,
            message=commit_input.commit_message,
            author=str(commit_input.author_ref),
            tenant=input_data.tenant_context,
        )
    except CommitStorageError as e:
        # Failure Mode: Commit storage failure
        return ExecutionOutput(
            result=None,
            error_code="COMMIT_STORAGE_FAILURE",
            error_message=f"Failed to store commit: {e}",
        )

    return ExecutionOutput(
        result=CommitResult(
            commit_id=commit_id,
            changeset_summary=changeset,
            committed_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class DatasetReadError(Exception):
    """Failed to read dataset."""


class CommitStorageError(Exception):
    """Failed to store commit."""
