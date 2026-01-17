# Data Fabric Capability Specification for Downstream Domains

Version: 1.0.0  
Status: Active  
Last Updated: 2026-01-16

## Overview

This document specifies how downstream domains (Model Foundry, Agent Platform, Business Operations) can discover and consume Data Fabric capabilities via MCOP.

## Capability Discovery

### Registry Query Pattern

Downstream domains discover Data Fabric capabilities by querying the MCOP Registry:

```
GET /registry/capabilities?domain=data_fabric
```

### Available Capabilities

| Capability Tag | Execution Unit | Description |
|----------------|----------------|-------------|
| `data_asset_registration` | DataAssetRegistrar | Register new data assets |
| `connection_testing` | ConnectionProbe | Test data source connectivity |
| `schema_discovery` | SchemaIntrospector | Discover schemas from sources |
| `data_extraction` | DataExtractor | Extract data from sources |
| `data_writing` | DataWriter | Write data to targets |
| `data_transformation` | TransformExecutor | Execute transformations |
| `data_joining` | DataJoiner | Join datasets |
| `data_aggregation` | AggregationComputer | Compute aggregations |
| `feature_computation` | FeatureComputer | Compute features |
| `feature_storage` | FeatureStoreWriter | Store features |
| `feature_retrieval` | FeatureRetriever | Retrieve features |
| `data_profiling` | DataProfiler | Profile data quality |
| `schema_validation` | SchemaValidator | Validate schemas |
| `data_versioning` | DataCommitter | Version data commits |
| `branch_management` | BranchCreator | Create data branches |
| `merge_computation` | MergeComputer | Merge data branches |
| `data_replication` | DataReplicator | Replicate data across clusters |
| `locality_signals` | LocalitySignalGenerator | Generate placement signals |
| `labeling_tasks` | LabelTaskCreator | Create labeling tasks |
| `label_recording` | LabelRecorder | Record labels |
| `lineage_tracking` | LineageEdgeWriter | Track data lineage |
| `quality_evaluation` | QualityGateEvaluator | Evaluate quality gates |

## Consumption Patterns

### Pattern 1: Model Foundry - Training Data Access

**Use Case**: Model Foundry needs training datasets for model training.

**Intent Flow**:
```
1. Model Foundry submits: RetrieveFeatures intent
2. Data Fabric: FeatureRetriever executes
3. Output: Feature vectors returned to Model Foundry
```

**Input Contract**:
```json
{
  "intent": "RetrieveFeatures",
  "inputs": {
    "feature_set_name": "customer_rfm",
    "entity_ids": ["c001", "c002", "c003"],
    "feature_names": ["recency", "frequency", "monetary"],
    "point_in_time": "2026-01-15T00:00:00Z"
  },
  "tenant_context": {
    "organization_id": "org-123",
    "workspace_id": "ws-456",
    "user_id": "user-789"
  }
}
```

**Output Contract**:
```json
{
  "status": "retrieved",
  "features": {
    "c001": {"recency": 5, "frequency": 12, "monetary": 1500.00},
    "c002": {"recency": 2, "frequency": 8, "monetary": 980.50},
    "c003": {"recency": 15, "frequency": 3, "monetary": 250.00}
  },
  "feature_reference": "ref-abc123"
}
```

### Pattern 2: Model Foundry - Dataset Lineage

**Use Case**: Model Foundry needs to track which datasets were used for training.

**Intent Flow**:
```
1. Model Foundry queries: QueryLocality intent
2. Data Fabric: LocalitySignalGenerator provides dataset metadata
3. Model Foundry records lineage
```

### Pattern 3: Agent Platform - Data Context

**Use Case**: AI Agents need data context for decision-making.

**Intent Flow**:
```
1. Agent submits: ProfileData intent
2. Data Fabric: DataProfiler executes
3. Output: Data profile returned to Agent
```

**Input Contract**:
```json
{
  "intent": "ProfileData",
  "inputs": {
    "data_reference": "lakehouse://curated/customer_360",
    "profile_config": {
      "sample_size": 10000,
      "compute_histograms": true,
      "compute_correlations": false
    }
  },
  "tenant_context": {...}
}
```

### Pattern 4: Business Operations - Data Quality Gates

**Use Case**: Business processes need data quality validation.

**Intent Flow**:
```
1. Business Ops submits: ValidateSchema intent
2. Data Fabric: SchemaValidator + QualityGateEvaluator execute
3. Output: Validation result with pass/fail
```

## Feedback Signals for Downstream Domains

### Signals Relevant to Model Foundry

| Signal | Type | Use Case |
|--------|------|----------|
| `metric_feature_materialization_latency` | Metric | Monitor feature freshness |
| `advisor_data_freshness` | Advisor | Training data staleness warnings |
| `outcome_data_quality_gate` | Outcome | Training data quality validation |

### Signals Relevant to Agent Platform

| Signal | Type | Use Case |
|--------|------|----------|
| `advisor_locality_placement` | Advisor | Data locality for agent placement |
| `outcome_connection_health` | Outcome | Data source availability |
| `metric_lineage_graph_growth` | Metric | Knowledge graph expansion |

### Signals Relevant to Business Operations

| Signal | Type | Use Case |
|--------|------|----------|
| `outcome_data_quality_gate` | Outcome | Process data validation |
| `outcome_schema_validation` | Outcome | Contract compliance |
| `metric_data_ingestion_volume` | Metric | Throughput monitoring |

## Intent Submission Protocol

### Via MCOP

```
POST /mcop/intents
Content-Type: application/json

{
  "domain": "data_fabric",
  "intent": "<intent_name>",
  "inputs": {...},
  "tenant_context": {...},
  "trace_id": "<optional-correlation-id>"
}
```

### Response Format

```json
{
  "intent_id": "int-uuid",
  "status": "accepted|rejected",
  "execution_id": "exec-uuid",
  "trace_id": "trace-uuid"
}
```

### Execution Status Query

```
GET /mcop/executions/{execution_id}
```

## Multi-Tenant Access Rules

1. **Organization Isolation**: Downstream domains can only access data within their organization
2. **Workspace Scoping**: Feature sets and datasets are workspace-scoped
3. **User Attribution**: All accesses are attributed to the requesting user
4. **Policy Enforcement**: Policy Engine evaluates access before execution

## Versioning and Compatibility

### API Version

- Current: v1
- Deprecation Policy: 6-month notice before breaking changes

### Schema Evolution

- Backward compatible changes: additive fields
- Breaking changes: require new intent version

## Error Handling

### Standard Error Codes

| Code | Description |
|------|-------------|
| `DATA_NOT_FOUND` | Requested dataset/feature not found |
| `ACCESS_DENIED` | Tenant does not have access |
| `VALIDATION_FAILED` | Input validation failed |
| `EXECUTION_FAILED` | Execution unit failed |
| `TIMEOUT` | Operation timed out |

### Error Response Format

```json
{
  "error": {
    "code": "DATA_NOT_FOUND",
    "message": "Feature set 'customer_rfm' not found",
    "details": {
      "feature_set": "customer_rfm",
      "tenant_context": {...}
    }
  }
}
```

## Rate Limiting

| Intent Category | Rate Limit |
|-----------------|------------|
| Read (Retrieve, Query) | 1000/min/tenant |
| Write (Register, Commit) | 100/min/tenant |
| Compute (Transform, Feature) | 50/min/tenant |

## Observability Integration

Downstream domains should subscribe to Data Fabric signals via the Observability Spine:

```
SUBSCRIBE /observability/signals
{
  "domain": "data_fabric",
  "signal_types": ["metric", "outcome", "advisor"],
  "tenant_filter": {...}
}
```

## Security Considerations

1. **Authentication**: All requests must include valid tenant context
2. **Authorization**: Policy Engine enforces access control
3. **Audit**: All data access is logged to Merkle Ledger
4. **Encryption**: Data in transit and at rest is encrypted

## Support and Contact

- Domain Owner: Data Platform Team
- Slack: #data-fabric-support
- Documentation: https://docs.aiverse.internal/data-fabric
