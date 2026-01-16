"""
Data Fabric Domain - Input/Output Contracts

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md
All contracts are declarative, stateless, and match ADR exactly.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID


class AssetType(str, Enum):
    """Data asset types owned by Data Fabric domain."""
    DATASET = "dataset"
    FEATURE_SET = "feature_set"
    LABEL_SET = "label_set"


class DataFormat(str, Enum):
    """Supported data formats."""
    PARQUET = "parquet"
    DELTA = "delta"
    ICEBERG = "iceberg"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"


class DataClassification(str, Enum):
    """Data classification levels."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"


class HealthStatus(str, Enum):
    """Connection health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class WriteMode(str, Enum):
    """Data write modes."""
    APPEND = "append"
    OVERWRITE = "overwrite"


class JoinType(str, Enum):
    """Join types for data joining."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class StoreType(str, Enum):
    """Feature store types."""
    OFFLINE = "offline"
    ONLINE = "online"


class ValidationMode(str, Enum):
    """Schema validation modes."""
    EXACT = "exact"
    COMPATIBLE = "compatible"
    SUBSET = "subset"


class ConsistencyMode(str, Enum):
    """Data consistency modes for replication."""
    EVENTUAL = "eventual"
    STRONG = "strong"


class MergeResult(str, Enum):
    """Merge operation results."""
    SUCCESS = "success"
    CONFLICT = "conflict"


class GateResult(str, Enum):
    """Quality gate results."""
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"


class LocalityType(str, Enum):
    """Data locality types."""
    LOCAL = "local"
    CACHED = "cached"
    REMOTE = "remote"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class TenantContext:
    """Immutable tenant context for all operations."""
    organization_id: UUID
    workspace_id: UUID
    user_id: UUID


@dataclass(frozen=True)
class AssetDeclaration:
    """Asset declaration for registration."""
    asset_type: AssetType
    name: str
    version: str
    schema_declaration: dict[str, Any]
    storage_location_ref: str
    classification: DataClassification
    data_format: DataFormat
    owner_ref: UUID


@dataclass(frozen=True)
class RegistrationResult:
    """Result of asset registration."""
    asset_id: UUID
    card_ref: str
    registered_at: datetime


@dataclass(frozen=True)
class ConnectionProbeInput:
    """Input for connection probe."""
    connection_ref: str
    credential_ref: str
    timeout_seconds: int


@dataclass(frozen=True)
class ConnectionProbeResult:
    """Result of connection probe."""
    health_status: HealthStatus
    latency_ms: int
    error_details: Optional[str]
    probed_at: datetime


@dataclass(frozen=True)
class SchemaIntrospectionInput:
    """Input for schema introspection."""
    connection_ref: str
    source_path: str
    sample_size: int


@dataclass(frozen=True)
class FieldDefinition:
    """Definition of a schema field."""
    name: str
    data_type: str
    nullable: bool
    is_key: bool


@dataclass(frozen=True)
class SchemaIntrospectionResult:
    """Result of schema introspection."""
    fields: list[FieldDefinition]
    primary_keys: list[str]
    row_count_estimate: int
    sample_values: dict[str, list[Any]]
    introspected_at: datetime


@dataclass(frozen=True)
class DataExtractionInput:
    """Input for data extraction."""
    source_connection_ref: str
    source_query_or_path: str
    extraction_offset: int
    extraction_limit: int
    output_format: DataFormat
    target_staging_ref: str


@dataclass(frozen=True)
class DataExtractionResult:
    """Result of data extraction."""
    bytes_extracted: int
    rows_extracted: int
    staging_ref: str
    watermark_value: Optional[str]
    extracted_at: datetime


@dataclass(frozen=True)
class DataWriteInput:
    """Input for data write."""
    staging_ref: str
    target_dataset_ref: str
    write_mode: WriteMode
    partition_spec: Optional[dict[str, Any]]


@dataclass(frozen=True)
class DataWriteResult:
    """Result of data write."""
    bytes_written: int
    rows_written: int
    target_location: str
    written_at: datetime


@dataclass(frozen=True)
class TransformInput:
    """Input for transformation."""
    input_data_ref: str
    transformation_definition: dict[str, Any]
    parameters: dict[str, Any]
    output_staging_ref: str


@dataclass(frozen=True)
class TransformResult:
    """Result of transformation."""
    rows_processed: int
    rows_output: int
    output_staging_ref: str
    transformation_hash: str
    transformed_at: datetime


@dataclass(frozen=True)
class JoinInput:
    """Input for data join."""
    left_input_ref: str
    right_input_ref: str
    join_keys: list[str]
    join_type: JoinType
    output_staging_ref: str


@dataclass(frozen=True)
class JoinResult:
    """Result of join operation."""
    rows_output: int
    matched_count: int
    unmatched_left: int
    unmatched_right: int
    output_staging_ref: str
    joined_at: datetime


@dataclass(frozen=True)
class AggregationInput:
    """Input for aggregation."""
    input_data_ref: str
    group_by_columns: list[str]
    aggregations: dict[str, str]
    output_staging_ref: str


@dataclass(frozen=True)
class AggregationResult:
    """Result of aggregation."""
    groups_computed: int
    output_staging_ref: str
    aggregated_at: datetime


@dataclass(frozen=True)
class FeatureComputeInput:
    """Input for feature computation."""
    source_data_ref: str
    feature_definition_ref: str
    entity_key_columns: list[str]
    time_start: datetime
    time_end: datetime
    output_staging_ref: str


@dataclass(frozen=True)
class FeatureComputeResult:
    """Result of feature computation."""
    entities_computed: int
    feature_values_count: int
    output_staging_ref: str
    computed_at: datetime


@dataclass(frozen=True)
class FeatureStoreWriteInput:
    """Input for feature store write."""
    staging_ref: str
    feature_set_ref: str
    store_type: StoreType
    ttl_seconds: int


@dataclass(frozen=True)
class FeatureStoreWriteResult:
    """Result of feature store write."""
    entities_written: int
    store_location: str
    written_at: datetime


@dataclass(frozen=True)
class FeatureRetrieveInput:
    """Input for feature retrieval."""
    feature_set_ref: str
    entity_keys: list[dict[str, Any]]
    feature_names: list[str]
    point_in_time: Optional[datetime]
    store_preference: StoreType


@dataclass(frozen=True)
class FeatureValue:
    """Single feature value."""
    entity_key: dict[str, Any]
    feature_name: str
    value: Any
    is_missing: bool
    staleness_seconds: int


@dataclass(frozen=True)
class FeatureRetrieveResult:
    """Result of feature retrieval."""
    values: list[FeatureValue]
    retrieved_at: datetime


@dataclass(frozen=True)
class ProfileInput:
    """Input for data profiling."""
    dataset_ref: str
    sample_size: int
    profiling_depth: str


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column."""
    column_name: str
    null_count: int
    distinct_count: int
    min_value: Optional[Any]
    max_value: Optional[Any]
    mean_value: Optional[float]


@dataclass(frozen=True)
class ProfileResult:
    """Result of data profiling."""
    column_stats: list[ColumnStatistics]
    quality_scores: dict[str, float]
    detected_patterns: list[str]
    profiled_at: datetime


@dataclass(frozen=True)
class SchemaValidationInput:
    """Input for schema validation."""
    dataset_ref: str
    expected_schema_ref: str
    validation_mode: ValidationMode


@dataclass(frozen=True)
class SchemaDiscrepancy:
    """Schema discrepancy details."""
    field_name: str
    expected_type: str
    actual_type: str
    issue: str


@dataclass(frozen=True)
class SchemaValidationResult:
    """Result of schema validation."""
    is_valid: bool
    discrepancies: list[SchemaDiscrepancy]
    validated_at: datetime


@dataclass(frozen=True)
class CommitInput:
    """Input for data commit."""
    dataset_ref: str
    parent_commit_ref: Optional[str]
    commit_message: str
    author_ref: UUID


@dataclass(frozen=True)
class CommitResult:
    """Result of commit operation."""
    commit_id: str
    changeset_summary: dict[str, int]
    committed_at: datetime


@dataclass(frozen=True)
class BranchInput:
    """Input for branch creation."""
    dataset_ref: str
    source_commit_ref: str
    branch_name: str


@dataclass(frozen=True)
class BranchResult:
    """Result of branch creation."""
    branch_id: UUID
    head_commit_ref: str
    created_at: datetime


@dataclass(frozen=True)
class MergeInput:
    """Input for merge computation."""
    source_commit_ref: str
    target_commit_ref: str
    common_ancestor_ref: str


@dataclass(frozen=True)
class MergeConflict:
    """Details of a merge conflict."""
    path: str
    source_value: Any
    target_value: Any


@dataclass(frozen=True)
class MergeComputeResult:
    """Result of merge computation."""
    result: MergeResult
    conflicts: list[MergeConflict]
    merged_changeset: Optional[dict[str, Any]]
    computed_at: datetime


@dataclass(frozen=True)
class ReplicationInput:
    """Input for data replication."""
    source_location_ref: str
    target_location_ref: str
    consistency_mode: ConsistencyMode


@dataclass(frozen=True)
class ReplicationResult:
    """Result of replication."""
    bytes_replicated: int
    target_confirmed: str
    checksum_match: bool
    replicated_at: datetime


@dataclass(frozen=True)
class LocalitySignal:
    """Locality signal for single environment."""
    environment_id: str
    locality_type: LocalityType
    transfer_cost_estimate: float
    confidence: float


@dataclass(frozen=True)
class LocalityResult:
    """Result of locality signal generation."""
    signals: list[LocalitySignal]
    signal_freshness: datetime


@dataclass(frozen=True)
class LabelTaskInput:
    """Input for label task creation."""
    source_dataset_ref: str
    sample_criteria: dict[str, Any]
    label_schema_ref: str
    quality_requirements: dict[str, float]


@dataclass(frozen=True)
class LabelTaskResult:
    """Result of label task creation."""
    task_id: UUID
    sample_count: int
    status: str
    created_at: datetime


@dataclass(frozen=True)
class LabelRecordInput:
    """Input for label recording."""
    task_ref: str
    sample_id: str
    label_value: Any
    annotator_ref: UUID


@dataclass(frozen=True)
class LabelRecordResult:
    """Result of label recording."""
    annotation_id: UUID
    recorded_at: datetime


@dataclass(frozen=True)
class LineageEdgeInput:
    """Input for lineage edge creation."""
    source_asset_ref: str
    target_asset_ref: str
    relationship_type: str
    execution_ref: str


@dataclass(frozen=True)
class LineageEdgeResult:
    """Result of lineage edge creation."""
    edge_id: UUID
    created_at: datetime


@dataclass(frozen=True)
class QualityGateInput:
    """Input for quality gate evaluation."""
    dataset_ref: str
    quality_rules_ref: str
    thresholds: dict[str, float]


@dataclass(frozen=True)
class QualityViolation:
    """Quality gate violation."""
    rule_name: str
    expected: float
    actual: float


@dataclass(frozen=True)
class QualityGateResult:
    """Result of quality gate evaluation."""
    result: GateResult
    metric_values: dict[str, float]
    violations: list[QualityViolation]
    evaluated_at: datetime
