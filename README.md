# AIVerse Data Fabric Domain Plugin

## Overview

This package implements the **Data Fabric** domain as a fully removable plugin for AIVerse.
All execution units are stateless, MCOP-scheduled, and adhere to the Phase 3 ADR specification.

**ADR Reference:** `docs/adr/PHASE3-DATA-FABRIC-PLUGIN-SPEC.md`

## Architecture Principles

### Domain Independence
- This domain can be **fully removed** without breaking the Control Plane or other domains
- No cross-domain calls or dependencies
- All interactions via published Control Plane interfaces only

### Stateless Execution
- All 22 execution units are **stateless**
- Terminable at any point with no side effects
- No internal caching, sessions, or accumulated state

### MCOP Scheduling
- All execution is MCOP-scheduled via the Control Plane
- No direct Kubernetes or cluster interactions
- Capability-based scheduling through abstract interfaces

## Structure

```
aiverse-platform-data-fabric/
  execution_units/         # 22 stateless execution units
    eu_01_data_asset_registrar.py
    eu_02_connection_probe.py
    ...
  registry_cards/          # Registry card metadata for each EU
    eu_01_data_asset_registrar_card.json
    ...
  feedback_signals/        # Metrics, outcomes, and advisors
    metric_*.json
    outcome_*.json
    advisor_*.json
  schemas/                 # Input/output contracts
    contracts.py
```

## Execution Units

| EU | Name | Capability | Consumer Intent |
|----|------|------------|-----------------|
| 01 | DataAssetRegistrar | Registers data assets | RegisterDataAsset |
| 02 | ConnectionProbe | Tests connections | TestConnection |
| 03 | SchemaIntrospector | Extracts schemas | DiscoverSchema |
| 04 | DataExtractor | Reads from source | IngestData |
| 05 | DataWriter | Writes to dataset | IngestData, TransformData |
| 06 | TransformExecutor | Applies transformations | TransformData |
| 07 | DataJoiner | Joins datasets | TransformData |
| 08 | AggregationComputer | Computes aggregates | TransformData |
| 09 | FeatureComputer | Computes features | MaterializeFeatures |
| 10 | FeatureStoreWriter | Writes to feature store | MaterializeFeatures |
| 11 | FeatureRetriever | Retrieves features | RetrieveFeatures |
| 12 | DataProfiler | Profiles datasets | ProfileData |
| 13 | SchemaValidator | Validates schemas | ValidateSchema |
| 14 | DataCommitter | Creates commits | CommitDataVersion |
| 15 | BranchCreator | Creates branches | BranchDataset |
| 16 | MergeComputer | Computes merges | MergeDataBranches |
| 17 | DataReplicator | Replicates data | ReplicateData |
| 18 | LocalitySignalGenerator | Generates locality signals | QueryLocality |
| 19 | LabelTaskCreator | Creates label tasks | CreateLabelTask |
| 20 | LabelRecorder | Records labels | CreateLabelTask |
| 21 | LineageEdgeWriter | Records lineage | IngestData, TransformData |
| 22 | QualityGateEvaluator | Evaluates quality gates | TransformData, IngestData |

## Feedback Signals

### Metrics
- `DataIngestionVolume` - Tracks ingestion volume
- `TransformationThroughput` - Tracks transformation performance
- `FeatureMaterializationLatency` - Tracks feature pipeline latency
- `LineageGraphGrowth` - Tracks lineage graph complexity

### Outcomes
- `DataQualityGateOutcome` - Quality gate pass/fail results
- `SchemaValidationOutcome` - Schema validation results
- `ConnectionHealthOutcome` - Connection health test results

### Advisors
- `LocalityPlacementAdvisor` - Placement recommendations for MCOP
- `DataFreshnessAdvisor` - Freshness and staleness advisories
- `LabelingCapacityAdvisor` - Labeling task progress estimates

## Removal Test

To verify domain independence, follow the removal checklist in the ADR:

1. Delete all files under this directory
2. Remove DataIntent type from intent enumeration
3. Remove data-related cards from registry seed data
4. Remove data-related policies from policy seed data
5. Verify Control Plane health check returns 200
6. Verify other intents (compute, model, agent) process normally
7. Verify no import errors or foreign key violations

If any step fails, the architecture has domain coupling that must be fixed.

## Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy .

# Linting
ruff check .
```

## License

Proprietary - Cisco Systems
