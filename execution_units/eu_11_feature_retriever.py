"""
EU-11: FeatureRetriever

Capability: Reads feature values for entities from store.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.11

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Retrieves features only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Store read failure: No features returned
- Entity not found: Null with indicator
- Feature not materialized: Missing indicator
- Store unavailable: Cannot proceed
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    FeatureRetrieveInput,
    FeatureRetrieveResult,
    FeatureValue,
    TenantContext,
)


class FeatureStoreClient(Protocol):
    """Protocol for feature store. Injected by Control Plane."""

    def read_features(
        self,
        feature_set_ref: str,
        entity_keys: list[dict[str, Any]],
        feature_names: list[str],
        point_in_time: datetime | None,
        store_type: str,
        tenant: TenantContext,
    ) -> list[dict[str, Any]]:
        """
        Read features. Returns list of feature records.
        Each record: {entity_key, feature_name, value, is_missing, staleness_seconds}
        """
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for FeatureRetriever."""
    retrieve_input: FeatureRetrieveInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for FeatureRetriever."""
    result: FeatureRetrieveResult | None
    error_code: str | None
    error_message: str | None


def execute(
    input_data: ExecutionInput,
    feature_store_client: FeatureStoreClient,
) -> ExecutionOutput:
    """
    Execute FeatureRetriever.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    retrieve_input = input_data.retrieve_input

    # Retrieve from feature store
    try:
        raw_features = feature_store_client.read_features(
            feature_set_ref=retrieve_input.feature_set_ref,
            entity_keys=retrieve_input.entity_keys,
            feature_names=retrieve_input.feature_names,
            point_in_time=retrieve_input.point_in_time,
            store_type=retrieve_input.store_preference.value,
            tenant=input_data.tenant_context,
        )
    except StoreReadError as e:
        # Failure Mode: Store read failure
        return ExecutionOutput(
            result=None,
            error_code="STORE_READ_FAILURE",
            error_message=f"Failed to read from feature store: {e}",
        )
    except StoreUnavailableError:
        # Failure Mode: Store unavailable
        return ExecutionOutput(
            result=None,
            error_code="STORE_UNAVAILABLE",
            error_message="Feature store is unavailable",
        )

    # Convert to FeatureValue objects
    # Failure modes "Entity not found" and "Feature not materialized"
    # are handled via the is_missing indicator in the response
    feature_values = [
        FeatureValue(
            entity_key=f["entity_key"],
            feature_name=f["feature_name"],
            value=f.get("value"),
            is_missing=f.get("is_missing", False),
            staleness_seconds=f.get("staleness_seconds", 0),
        )
        for f in raw_features
    ]

    return ExecutionOutput(
        result=FeatureRetrieveResult(
            values=feature_values,
            retrieved_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
    )


# Exception types for failure modes
class StoreReadError(Exception):
    """Failed to read from feature store."""


class StoreUnavailableError(Exception):
    """Feature store is unavailable."""
