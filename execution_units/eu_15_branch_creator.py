"""
EU-15: BranchCreator

Capability: Creates branch record pointing to commit.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.15

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Creates branches only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Commit not found: Rejected
- Name conflict: Rejected
- Registry write failure: No branch
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from ..schemas import (
    BranchInput,
    BranchResult,
    TenantContext,
)


class BranchRegistry(Protocol):
    """Protocol for branch registry. Injected by Control Plane."""

    def create_branch(
        self,
        dataset_ref: str,
        branch_name: str,
        head_commit_ref: str,
        tenant: TenantContext,
    ) -> str:
        """Create branch. Returns branch identifier."""
        ...

    def branch_exists(
        self, dataset_ref: str, branch_name: str, tenant: TenantContext
    ) -> bool:
        """Check if branch exists."""
        ...


class CommitStore(Protocol):
    """Protocol for commit store. Injected by Control Plane."""

    def get_commit(
        self, commit_ref: str, tenant: TenantContext
    ) -> dict[str, Any] | None:
        """Get commit by reference."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for BranchCreator."""
    branch_input: BranchInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for BranchCreator."""
    result: BranchResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    commit_store: CommitStore,
    branch_registry: BranchRegistry,
) -> ExecutionOutput:
    """
    Execute BranchCreator.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    branch_input = input_data.branch_input

    # Validate source commit exists
    commit = commit_store.get_commit(
        branch_input.source_commit_ref,
        input_data.tenant_context,
    )
    if commit is None:
        # Failure Mode: Commit not found
        return ExecutionOutput(
            result=None,
            error_code="COMMIT_NOT_FOUND",
            error_message=f"Source commit not found: {branch_input.source_commit_ref}",
        )

    # Check for name conflict
    if branch_registry.branch_exists(
        branch_input.dataset_ref,
        branch_input.branch_name,
        input_data.tenant_context,
    ):
        # Failure Mode: Name conflict
        return ExecutionOutput(
            result=None,
            error_code="NAME_CONFLICT",
            error_message=f"Branch already exists: {branch_input.branch_name}",
        )

    # Create branch
    try:
        branch_id_str = branch_registry.create_branch(
            dataset_ref=branch_input.dataset_ref,
            branch_name=branch_input.branch_name,
            head_commit_ref=branch_input.source_commit_ref,
            tenant=input_data.tenant_context,
        )
    except RegistryWriteError as e:
        # Failure Mode: Registry write failure
        return ExecutionOutput(
            result=None,
            error_code="REGISTRY_WRITE_FAILURE",
            error_message=f"Failed to create branch: {e}",
        )

    return ExecutionOutput(
        result=BranchResult(
            branch_id=uuid4(),  # Use actual ID from registry in production
            head_commit_ref=branch_input.source_commit_ref,
            created_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class RegistryWriteError(Exception):
    """Failed to write to registry."""
