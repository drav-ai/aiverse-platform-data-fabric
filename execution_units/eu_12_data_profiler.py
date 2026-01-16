"""
EU-12: DataProfiler

Capability: Computes statistical profile and quality metrics.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.12

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Profiles data only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Dataset read failure: No profile
- Sample too small: Low confidence indicator
- Timeout: No partial profile
- Invalid dataset: Rejected
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    ColumnStatistics,
    ProfileInput,
    ProfileResult,
    TenantContext,
)


class ProfileEngine(Protocol):
    """Protocol for profiling engine. Injected by Control Plane."""

    def compute_profile(
        self,
        dataset_data: bytes,
        sample_size: int,
        profiling_depth: str,
    ) -> tuple[list[dict[str, Any]], dict[str, float], list[str], bool]:
        """
        Compute profile.
        Returns (column_stats, quality_scores, patterns, low_confidence).
        """
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
    """Input contract for DataProfiler."""
    profile_input: ProfileInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for DataProfiler."""
    result: ProfileResult | None
    error_code: str | None
    error_message: str | None
    low_confidence: bool


def execute(
    input_data: ExecutionInput,
    dataset_reader: DatasetReader,
    profile_engine: ProfileEngine,
) -> ExecutionOutput:
    """
    Execute DataProfiler.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    profile_input = input_data.profile_input

    # Read dataset
    try:
        dataset_data = dataset_reader.read_dataset(
            profile_input.dataset_ref,
            input_data.tenant_context,
        )
    except DatasetReadError as e:
        # Failure Mode: Dataset read failure
        return ExecutionOutput(
            result=None,
            error_code="DATASET_READ_FAILURE",
            error_message=f"Failed to read dataset: {e}",
            low_confidence=False,
        )
    except InvalidDatasetError as e:
        # Failure Mode: Invalid dataset
        return ExecutionOutput(
            result=None,
            error_code="INVALID_DATASET",
            error_message=f"Invalid dataset: {e}",
            low_confidence=False,
        )

    # Compute profile
    try:
        raw_stats, quality_scores, patterns, low_confidence = (
            profile_engine.compute_profile(
                dataset_data=dataset_data,
                sample_size=profile_input.sample_size,
                profiling_depth=profile_input.profiling_depth,
            )
        )
    except ProfileTimeoutError:
        # Failure Mode: Timeout - no partial profile
        return ExecutionOutput(
            result=None,
            error_code="PROFILE_TIMEOUT",
            error_message="Profiling timed out",
            low_confidence=False,
        )

    # Convert to ColumnStatistics
    column_stats = [
        ColumnStatistics(
            column_name=s["column_name"],
            null_count=s["null_count"],
            distinct_count=s["distinct_count"],
            min_value=s.get("min_value"),
            max_value=s.get("max_value"),
            mean_value=s.get("mean_value"),
        )
        for s in raw_stats
    ]

    return ExecutionOutput(
        result=ProfileResult(
            column_stats=column_stats,
            quality_scores=quality_scores,
            detected_patterns=patterns,
            profiled_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
        low_confidence=low_confidence,  # Failure Mode: Sample too small
    )


# Exception types for failure modes
class DatasetReadError(Exception):
    """Failed to read dataset."""


class InvalidDatasetError(Exception):
    """Dataset is invalid."""


class ProfileTimeoutError(Exception):
    """Profiling timed out."""
