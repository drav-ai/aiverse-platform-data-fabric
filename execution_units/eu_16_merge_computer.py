"""
EU-16: MergeComputer

Capability: Computes merge between two commits, identifies conflicts.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.16

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Computes merges only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Commit read failure: No merge
- No common ancestor: Rejected
- Unresolvable conflicts: Conflict report returned
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    MergeComputeResult,
    MergeConflict,
    MergeInput,
    MergeResult,
    TenantContext,
)


class MergeEngine(Protocol):
    """Protocol for merge engine. Injected by Control Plane."""

    def compute_merge(
        self,
        source_content: bytes,
        target_content: bytes,
        ancestor_content: bytes,
    ) -> tuple[bool, list[dict[str, Any]], dict[str, Any] | None]:
        """
        Compute merge.
        Returns (success, conflicts, merged_changeset).
        """
        ...


class CommitStore(Protocol):
    """Protocol for commit store. Injected by Control Plane."""

    def get_commit(
        self, commit_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get commit by reference."""
        ...

    def get_commit_content(
        self, commit_ref: str, tenant: TenantContext
    ) -> bytes:
        """Get commit content."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for MergeComputer."""
    merge_input: MergeInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for MergeComputer."""
    result: MergeComputeResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    commit_store: CommitStore,
    merge_engine: MergeEngine,
) -> ExecutionOutput:
    """
    Execute MergeComputer.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    merge_input = input_data.merge_input

    # Validate commits exist
    source = commit_store.get_commit(
        merge_input.source_commit_ref,
        input_data.tenant_context,
    )
    if source is None:
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_NOT_FOUND",
            error_message=f"Source commit not found: {merge_input.source_commit_ref}",
        )

    target = commit_store.get_commit(
        merge_input.target_commit_ref,
        input_data.tenant_context,
    )
    if target is None:
        return ExecutionOutput(
            result=None,
            error_code="TARGET_NOT_FOUND",
            error_message=f"Target commit not found: {merge_input.target_commit_ref}",
        )

    ancestor = commit_store.get_commit(
        merge_input.common_ancestor_ref,
        input_data.tenant_context,
    )
    if ancestor is None:
        # Failure Mode: No common ancestor
        return ExecutionOutput(
            result=None,
            error_code="NO_COMMON_ANCESTOR",
            error_message="No common ancestor found for merge",
        )

    # Read commit contents
    try:
        source_content = commit_store.get_commit_content(
            merge_input.source_commit_ref,
            input_data.tenant_context,
        )
        target_content = commit_store.get_commit_content(
            merge_input.target_commit_ref,
            input_data.tenant_context,
        )
        ancestor_content = commit_store.get_commit_content(
            merge_input.common_ancestor_ref,
            input_data.tenant_context,
        )
    except CommitReadError as e:
        # Failure Mode: Commit read failure
        return ExecutionOutput(
            result=None,
            error_code="COMMIT_READ_FAILURE",
            error_message=f"Failed to read commit content: {e}",
        )

    # Compute merge
    success, raw_conflicts, merged_changeset = merge_engine.compute_merge(
        source_content=source_content,
        target_content=target_content,
        ancestor_content=ancestor_content,
    )

    # Convert to MergeConflict objects
    conflicts = [
        MergeConflict(
            path=c["path"],
            source_value=c["source_value"],
            target_value=c["target_value"],
        )
        for c in raw_conflicts
    ]

    # Failure Mode: Unresolvable conflicts - return conflict report
    merge_result = MergeResult.SUCCESS if success else MergeResult.CONFLICT

    return ExecutionOutput(
        result=MergeComputeResult(
            result=merge_result,
            conflicts=conflicts,
            merged_changeset=merged_changeset,
            computed_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class CommitReadError(Exception):
    """Failed to read commit content."""
