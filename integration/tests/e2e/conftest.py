"""
E2E Test Fixtures for Data Fabric Domain Validation
"""
import pytest
from typing import Dict, Any, List, Optional
from uuid import uuid4
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path


@dataclass
class TenantContext:
    """Multi-tenant context for test isolation."""
    organization_id: str = field(default_factory=lambda: str(uuid4()))
    workspace_id: str = field(default_factory=lambda: str(uuid4()))
    user_id: str = field(default_factory=lambda: str(uuid4()))
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "organization_id": self.organization_id,
            "workspace_id": self.workspace_id,
            "user_id": self.user_id,
        }


@dataclass
class IntentSubmission:
    """Represents an intent submission for testing."""
    intent_name: str
    inputs: Dict[str, Any]
    tenant_context: TenantContext
    expected_execution_units: List[str]
    expected_signals: List[str]
    
    
@dataclass
class ExecutionResult:
    """Result of an intent execution."""
    intent_id: str
    status: str
    execution_unit: str
    duration_ms: int
    signals_emitted: List[str]
    observability_events: List[Dict[str, Any]]
    policy_evaluations: List[Dict[str, Any]]


@dataclass
class SignalCapture:
    """Captured feedback signal for validation."""
    signal_name: str
    signal_type: str
    value: Any
    timestamp: str
    tenant_context: Dict[str, str]


class MockMCOPRegistry:
    """Mock MCOP Registry for E2E testing."""
    
    def __init__(self):
        self._cards: Dict[str, Dict[str, Any]] = {}
        self._capabilities: Dict[str, List[str]] = {}
        
    def load_card(self, card_path: Path) -> Dict[str, Any]:
        """Load a registry card."""
        with open(card_path) as f:
            card = json.load(f)
        # Handle both flat and nested name structures
        name = card.get("name") or card.get("metadata", {}).get("name")
        if not name:
            raise KeyError(f"No 'name' found in card: {card_path}")
        card["name"] = name  # Normalize to flat structure for tests
        self._cards[name] = card
        # Handle both flat capability_tags and nested capability.tags
        tags = card.get("capability_tags", []) or card.get("capability", {}).get("tags", [])
        for tag in tags:
            if tag not in self._capabilities:
                self._capabilities[tag] = []
            self._capabilities[tag].append(name)
        return card
    
    def get_card(self, name: str) -> Optional[Dict[str, Any]]:
        return self._cards.get(name)
    
    def list_capabilities(self) -> Dict[str, List[str]]:
        return self._capabilities
    
    def get_units_for_capability(self, capability: str) -> List[str]:
        return self._capabilities.get(capability, [])


class MockObservabilitySpine:
    """Mock Observability Spine for signal capture."""
    
    def __init__(self):
        self._events: List[Dict[str, Any]] = []
        self._signals: List[SignalCapture] = []
        
    def emit_event(self, event: Dict[str, Any]) -> None:
        self._events.append({
            **event,
            "captured_at": datetime.now(timezone.utc).isoformat()
        })
        
    def emit_signal(self, signal: SignalCapture) -> None:
        self._signals.append(signal)
        
    def get_events(self, trace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if trace_id:
            return [e for e in self._events if e.get("trace_id") == trace_id]
        return self._events
    
    def get_signals(self, signal_type: Optional[str] = None) -> List[SignalCapture]:
        if signal_type:
            return [s for s in self._signals if s.signal_type == signal_type]
        return self._signals
    
    def clear(self) -> None:
        self._events.clear()
        self._signals.clear()


class MockPolicyEngine:
    """Mock Policy Engine for policy enforcement testing."""
    
    def __init__(self):
        self._policies: Dict[str, Dict[str, Any]] = {}
        self._evaluations: List[Dict[str, Any]] = []
        
    def register_policy(self, policy_id: str, policy: Dict[str, Any]) -> None:
        self._policies[policy_id] = policy
        
    def evaluate(self, intent_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            "intent": intent_name,
            "context": context,
            "policies_evaluated": list(self._policies.keys()),
            "decision": "allow",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self._evaluations.append(result)
        return result
    
    def get_evaluations(self) -> List[Dict[str, Any]]:
        return self._evaluations


@pytest.fixture
def mock_mcop_registry():
    """Provide a mock MCOP registry."""
    return MockMCOPRegistry()


@pytest.fixture
def mock_observability():
    """Provide a mock observability spine."""
    return MockObservabilitySpine()


@pytest.fixture
def mock_policy_engine():
    """Provide a mock policy engine."""
    return MockPolicyEngine()


@pytest.fixture
def tenant_a():
    """First test tenant."""
    return TenantContext(
        organization_id="org-tenant-a",
        workspace_id="ws-tenant-a",
        user_id="user-tenant-a"
    )


@pytest.fixture
def tenant_b():
    """Second test tenant for isolation testing."""
    return TenantContext(
        organization_id="org-tenant-b",
        workspace_id="ws-tenant-b",
        user_id="user-tenant-b"
    )


@pytest.fixture
def registry_cards_path():
    """Path to registry cards."""
    return Path(__file__).parent.parent.parent.parent / "registry_cards"


@pytest.fixture
def feedback_signals_path():
    """Path to feedback signals."""
    return Path(__file__).parent.parent.parent.parent / "feedback_signals"


@pytest.fixture
def all_intent_definitions() -> List[Dict[str, Any]]:
    """All 15 Data Fabric intent definitions."""
    return [
        {
            "name": "RegisterDataAsset",
            "execution_units": ["DataAssetRegistrar"],
            "signals": ["metric_data_asset_registration_count"],
            "sample_inputs": {
                "asset_name": "customer_transactions",
                "source_system": "postgres_main",
                "schema_reference": "schema_v1"
            }
        },
        {
            "name": "IngestData",
            "execution_units": ["DataExtractor", "DataWriter"],
            "signals": ["metric_data_ingestion_volume", "outcome_data_quality_gate"],
            "sample_inputs": {
                "source_connection": "conn_postgres",
                "target_location": "lakehouse://raw/transactions",
                "extraction_spec": {"mode": "full"}
            }
        },
        {
            "name": "TransformData",
            "execution_units": ["TransformExecutor", "DataJoiner", "AggregationComputer"],
            "signals": ["metric_transformation_throughput"],
            "sample_inputs": {
                "input_datasets": ["raw_transactions", "raw_customers"],
                "transformation_spec": {"type": "join", "keys": ["customer_id"]}
            }
        },
        {
            "name": "MaterializeFeatures",
            "execution_units": ["FeatureComputer", "FeatureStoreWriter"],
            "signals": ["metric_feature_materialization_latency"],
            "sample_inputs": {
                "feature_set": "customer_rfm",
                "target_store": "feature_store_v1"
            }
        },
        {
            "name": "RetrieveFeatures",
            "execution_units": ["FeatureRetriever"],
            "signals": [],
            "sample_inputs": {
                "feature_set": "customer_rfm",
                "entity_ids": ["c001", "c002"]
            }
        },
        {
            "name": "ProfileData",
            "execution_units": ["DataProfiler"],
            "signals": ["advisor_data_freshness"],
            "sample_inputs": {
                "dataset": "raw_transactions",
                "profile_config": {"sample_size": 10000}
            }
        },
        {
            "name": "CommitDataVersion",
            "execution_units": ["DataCommitter"],
            "signals": [],
            "sample_inputs": {
                "dataset": "curated_transactions",
                "version_message": "Add fraud flag column"
            }
        },
        {
            "name": "BranchDataset",
            "execution_units": ["BranchCreator"],
            "signals": [],
            "sample_inputs": {
                "source_branch": "main",
                "new_branch": "experiment_v2"
            }
        },
        {
            "name": "MergeDataBranches",
            "execution_units": ["MergeComputer"],
            "signals": [],
            "sample_inputs": {
                "source_branch": "experiment_v2",
                "target_branch": "main",
                "strategy": "fast_forward"
            }
        },
        {
            "name": "CreateLabelTask",
            "execution_units": ["LabelTaskCreator"],
            "signals": ["advisor_labeling_capacity"],
            "sample_inputs": {
                "dataset": "raw_images",
                "label_schema": "object_detection_v1",
                "sample_count": 1000
            }
        },
        {
            "name": "TestConnection",
            "execution_units": ["ConnectionProbe"],
            "signals": ["outcome_connection_health"],
            "sample_inputs": {
                "connection_id": "conn_postgres",
                "test_type": "full"
            }
        },
        {
            "name": "DiscoverSchema",
            "execution_units": ["SchemaIntrospector"],
            "signals": ["outcome_schema_validation"],
            "sample_inputs": {
                "connection_id": "conn_postgres",
                "table_pattern": "public.*"
            }
        },
        {
            "name": "ReplicateData",
            "execution_units": ["DataReplicator"],
            "signals": ["metric_data_ingestion_volume"],
            "sample_inputs": {
                "source_dataset": "raw_transactions",
                "target_cluster": "cluster_eu_west"
            }
        },
        {
            "name": "QueryLocality",
            "execution_units": ["LocalitySignalGenerator"],
            "signals": ["advisor_locality_placement"],
            "sample_inputs": {
                "dataset": "raw_transactions",
                "query_type": "placement_hint"
            }
        },
        {
            "name": "ValidateSchema",
            "execution_units": ["SchemaValidator", "QualityGateEvaluator"],
            "signals": ["outcome_schema_validation", "outcome_data_quality_gate"],
            "sample_inputs": {
                "dataset": "raw_transactions",
                "expected_schema": "schema_v1"
            }
        }
    ]


@pytest.fixture
def byot_tool_configs() -> Dict[str, List[Dict[str, Any]]]:
    """BYOT tool configurations for replacement testing."""
    return {
        "ingestion": [
            {"tool": "airbyte_oss", "version": "0.50+", "type": "open_source"},
            {"tool": "fivetran", "version": "latest", "type": "saas"},
            {"tool": "custom_cdc", "version": "1.0", "type": "custom"}
        ],
        "transformation": [
            {"tool": "dbt_core", "version": "1.7+", "type": "open_source"},
            {"tool": "dbt_cloud", "version": "latest", "type": "saas"},
            {"tool": "spark_sql", "version": "3.5+", "type": "open_source"}
        ],
        "feature_store": [
            {"tool": "feast", "version": "0.35+", "type": "open_source"},
            {"tool": "tecton", "version": "latest", "type": "saas"},
            {"tool": "custom_store", "version": "1.0", "type": "custom"}
        ],
        "versioning": [
            {"tool": "lakefs", "version": "1.0+", "type": "open_source"},
            {"tool": "nessie", "version": "latest", "type": "open_source"},
            {"tool": "delta_lake", "version": "3.0+", "type": "open_source"}
        ]
    }
