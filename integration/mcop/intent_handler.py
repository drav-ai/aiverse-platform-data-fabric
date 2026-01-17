"""
Intent Handler for Data Fabric Domain.

Maps Data Fabric intents to execution units per ADR Section 4.

Per ADR: No orchestration - MCOP handles sequencing.
"""

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True)
class ExecutionUnitRef:
    """Reference to an execution unit for scheduling."""
    name: str
    capability_type: str
    input_mapping: dict[str, str]


# Intent to execution unit mapping per ADR Section 4
INTENT_TO_EXECUTION_UNITS: dict[str, list[ExecutionUnitRef]] = {
    "RegisterDataAsset": [
        ExecutionUnitRef(
            name="DataAssetRegistrar",
            capability_type="data-registration",
            input_mapping={"asset_declaration": "asset_declaration"},
        ),
    ],
    "IngestData": [
        ExecutionUnitRef(
            name="DataExtractor",
            capability_type="data-extraction",
            input_mapping={
                "source_connection_ref": "extraction_input.source_connection_ref",
                "source_query_or_path": "extraction_input.source_query_or_path",
            },
        ),
        ExecutionUnitRef(
            name="DataWriter",
            capability_type="data-writing",
            input_mapping={
                "staging_ref": "write_input.staging_ref",
                "target_dataset_ref": "write_input.target_dataset_ref",
            },
        ),
        ExecutionUnitRef(
            name="LineageEdgeWriter",
            capability_type="lineage-recording",
            input_mapping={
                "source_asset_ref": "edge_input.source_asset_ref",
                "target_asset_ref": "edge_input.target_asset_ref",
            },
        ),
    ],
    "TransformData": [
        ExecutionUnitRef(
            name="TransformExecutor",
            capability_type="data-transformation",
            input_mapping={
                "input_data_ref": "transform_input.input_data_ref",
                "transformation_definition": "transform_input.transformation_definition",
            },
        ),
        ExecutionUnitRef(
            name="DataWriter",
            capability_type="data-writing",
            input_mapping={
                "staging_ref": "write_input.staging_ref",
                "target_dataset_ref": "write_input.target_dataset_ref",
            },
        ),
        ExecutionUnitRef(
            name="LineageEdgeWriter",
            capability_type="lineage-recording",
            input_mapping={
                "source_asset_ref": "edge_input.source_asset_ref",
                "target_asset_ref": "edge_input.target_asset_ref",
            },
        ),
    ],
    "MaterializeFeatures": [
        ExecutionUnitRef(
            name="FeatureComputer",
            capability_type="feature-computation",
            input_mapping={
                "source_data_ref": "compute_input.source_data_ref",
                "feature_definition_ref": "compute_input.feature_definition_ref",
            },
        ),
        ExecutionUnitRef(
            name="FeatureStoreWriter",
            capability_type="feature-storage",
            input_mapping={
                "staging_ref": "write_input.staging_ref",
                "feature_set_ref": "write_input.feature_set_ref",
            },
        ),
    ],
    "RetrieveFeatures": [
        ExecutionUnitRef(
            name="FeatureRetriever",
            capability_type="feature-retrieval",
            input_mapping={
                "feature_set_ref": "retrieve_input.feature_set_ref",
                "entity_keys": "retrieve_input.entity_keys",
            },
        ),
    ],
    "ProfileData": [
        ExecutionUnitRef(
            name="DataProfiler",
            capability_type="data-profiling",
            input_mapping={
                "dataset_ref": "profile_input.dataset_ref",
                "sample_size": "profile_input.sample_size",
            },
        ),
    ],
    "CommitDataVersion": [
        ExecutionUnitRef(
            name="DataCommitter",
            capability_type="data-versioning",
            input_mapping={
                "dataset_ref": "commit_input.dataset_ref",
                "commit_message": "commit_input.commit_message",
            },
        ),
    ],
    "BranchDataset": [
        ExecutionUnitRef(
            name="BranchCreator",
            capability_type="data-branching",
            input_mapping={
                "dataset_ref": "branch_input.dataset_ref",
                "source_commit_ref": "branch_input.source_commit_ref",
            },
        ),
    ],
    "MergeDataBranches": [
        ExecutionUnitRef(
            name="MergeComputer",
            capability_type="data-merging",
            input_mapping={
                "source_commit_ref": "merge_input.source_commit_ref",
                "target_commit_ref": "merge_input.target_commit_ref",
            },
        ),
        ExecutionUnitRef(
            name="DataCommitter",
            capability_type="data-versioning",
            input_mapping={
                "dataset_ref": "commit_input.dataset_ref",
                "commit_message": "commit_input.commit_message",
            },
        ),
    ],
    "CreateLabelTask": [
        ExecutionUnitRef(
            name="LabelTaskCreator",
            capability_type="labeling-task",
            input_mapping={
                "source_dataset_ref": "task_input.source_dataset_ref",
                "label_schema_ref": "task_input.label_schema_ref",
            },
        ),
    ],
    "TestConnection": [
        ExecutionUnitRef(
            name="ConnectionProbe",
            capability_type="connection-testing",
            input_mapping={
                "connection_ref": "probe_input.connection_ref",
                "timeout_seconds": "probe_input.timeout_seconds",
            },
        ),
    ],
    "DiscoverSchema": [
        ExecutionUnitRef(
            name="SchemaIntrospector",
            capability_type="schema-discovery",
            input_mapping={
                "connection_ref": "introspection_input.connection_ref",
                "source_path": "introspection_input.source_path",
            },
        ),
    ],
    "ReplicateData": [
        ExecutionUnitRef(
            name="DataReplicator",
            capability_type="data-replication",
            input_mapping={
                "source_location_ref": "replication_input.source_location_ref",
                "target_location_ref": "replication_input.target_location_ref",
            },
        ),
    ],
    "QueryLocality": [
        ExecutionUnitRef(
            name="LocalitySignalGenerator",
            capability_type="locality-signaling",
            input_mapping={"asset_ref": "asset_ref"},
        ),
    ],
    "ValidateSchema": [
        ExecutionUnitRef(
            name="SchemaValidator",
            capability_type="schema-validation",
            input_mapping={
                "dataset_ref": "validation_input.dataset_ref",
                "expected_schema_ref": "validation_input.expected_schema_ref",
            },
        ),
    ],
}


class IntentEngine(Protocol):
    """Protocol for Intent Engine interaction."""

    async def decompose_intent(
        self,
        intent_id: UUID,
        execution_units: list[dict[str, Any]],
    ) -> bool:
        """Submit execution unit decomposition for an intent."""
        ...


class DataFabricIntentHandler:
    """
    Handles Data Fabric intents by mapping them to execution units.

    Per ADR Section 4:
    - Multiple EUs per intent are scheduled independently by MCOP
    - No orchestration occurs in this handler
    - Control Plane sequences based on intent decomposition
    """

    DOMAIN = "data-fabric"
    SUPPORTED_INTENTS = set(INTENT_TO_EXECUTION_UNITS.keys())

    def __init__(self, intent_engine: IntentEngine | None = None):
        self._intent_engine = intent_engine

    def get_execution_units_for_intent(
        self,
        intent_type: str,
    ) -> list[ExecutionUnitRef] | None:
        """
        Get execution units for an intent type.

        Returns None if intent is not supported by Data Fabric.
        """
        return INTENT_TO_EXECUTION_UNITS.get(intent_type)

    def is_supported_intent(self, intent_type: str) -> bool:
        """Check if an intent type is supported by Data Fabric."""
        return intent_type in self.SUPPORTED_INTENTS

    async def handle_intent(
        self,
        intent_id: UUID,
        intent_type: str,
        intent_params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Handle a Data Fabric intent by decomposing it to execution units.

        Returns decomposition result with execution unit references.

        Per ADR: This handler does NOT execute units - only decomposes.
        """
        if not self.is_supported_intent(intent_type):
            return {
                "success": False,
                "error": f"Unsupported intent type: {intent_type}",
                "domain": self.DOMAIN,
            }

        execution_units = self.get_execution_units_for_intent(intent_type)
        if not execution_units:
            return {
                "success": False,
                "error": f"No execution units mapped for intent: {intent_type}",
                "domain": self.DOMAIN,
            }

        # Build execution unit specs for MCOP
        eu_specs = [
            {
                "name": eu.name,
                "capability_type": eu.capability_type,
                "input_mapping": eu.input_mapping,
                "domain": self.DOMAIN,
            }
            for eu in execution_units
        ]

        # Submit to Intent Engine if available
        if self._intent_engine:
            try:
                await self._intent_engine.decompose_intent(
                    intent_id=intent_id,
                    execution_units=eu_specs,
                )
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to submit decomposition: {e}",
                    "domain": self.DOMAIN,
                }

        return {
            "success": True,
            "intent_id": str(intent_id),
            "intent_type": intent_type,
            "domain": self.DOMAIN,
            "execution_units": eu_specs,
            "unit_count": len(eu_specs),
        }

    def get_supported_intents(self) -> list[str]:
        """Get list of supported intent types."""
        return list(self.SUPPORTED_INTENTS)

    def get_intent_count(self) -> int:
        """Get count of supported intents."""
        return len(self.SUPPORTED_INTENTS)
