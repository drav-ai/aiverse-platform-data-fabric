"""
EU-03: SchemaIntrospector

Capability: Extracts schema metadata from data source.

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md Section 3.3

STATELESS - No internal state or caching.
MCOP-SCHEDULED - Scheduled via Control Plane.
SINGLE CAPABILITY - Introspects schemas only.
NO ORCHESTRATION - Does not call other units.

Failure Modes:
- Connection failure: No schema returned
- Access denied: Rejected with permission error
- Source not found: Rejected with not-found error
- Oversized schema: Truncated with indicator
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from ..schemas import (
    FieldDefinition,
    SchemaIntrospectionInput,
    SchemaIntrospectionResult,
    TenantContext,
)


class SchemaReader(Protocol):
    """Protocol for schema reading. Injected by Control Plane."""

    def read_schema(
        self,
        connection_ref: str,
        source_path: str,
        sample_size: int,
        tenant: TenantContext,
    ) -> tuple[list[dict], list[str], int, dict[str, list[Any]]]:
        """Read schema. Returns (fields, keys, row_count, samples)."""
        ...


@dataclass(frozen=True)
class ExecutionInput:
    """Input contract for SchemaIntrospector."""
    introspection_input: SchemaIntrospectionInput
    tenant_context: TenantContext


@dataclass(frozen=True)
class ExecutionOutput:
    """Output contract for SchemaIntrospector."""
    result: SchemaIntrospectionResult | None
    error_code: str | None
    error_message: str | None
    is_truncated: bool


MAX_FIELDS = 1000  # Schema truncation limit


def execute(
    input_data: ExecutionInput,
    schema_reader: SchemaReader,
) -> ExecutionOutput:
    """
    Execute SchemaIntrospector.

    Stateless, deterministic execution.
    All inputs provided explicitly.
    No side effects on failure.
    """
    intro_input = input_data.introspection_input

    try:
        raw_fields, keys, row_count, samples = schema_reader.read_schema(
            connection_ref=intro_input.connection_ref,
            source_path=intro_input.source_path,
            sample_size=intro_input.sample_size,
            tenant=input_data.tenant_context,
        )
    except ConnectionFailureError:
        # Failure Mode: Connection failure
        return ExecutionOutput(
            result=None,
            error_code="CONNECTION_FAILURE",
            error_message="Failed to connect to data source",
            is_truncated=False,
        )
    except AccessDeniedError:
        # Failure Mode: Access denied
        return ExecutionOutput(
            result=None,
            error_code="ACCESS_DENIED",
            error_message="Access denied to source",
            is_truncated=False,
        )
    except SourceNotFoundError:
        # Failure Mode: Source not found
        return ExecutionOutput(
            result=None,
            error_code="SOURCE_NOT_FOUND",
            error_message=f"Source not found: {intro_input.source_path}",
            is_truncated=False,
        )

    # Convert to field definitions
    fields = [
        FieldDefinition(
            name=f["name"],
            data_type=f["type"],
            nullable=f.get("nullable", True),
            is_key=f["name"] in keys,
        )
        for f in raw_fields
    ]

    # Failure Mode: Oversized schema - truncate
    is_truncated = len(fields) > MAX_FIELDS
    if is_truncated:
        fields = fields[:MAX_FIELDS]

    return ExecutionOutput(
        result=SchemaIntrospectionResult(
            fields=fields,
            primary_keys=keys,
            row_count_estimate=row_count,
            sample_values=samples,
            introspected_at=datetime.now(timezone.utc),
        ),
        error_code=None,
        error_message=None,
        is_truncated=is_truncated,
    )


# Exception types for failure modes
class ConnectionFailureError(Exception):
    """Connection to source failed."""


class AccessDeniedError(Exception):
    """Access to source denied."""


class SourceNotFoundError(Exception):
    """Source does not exist."""
