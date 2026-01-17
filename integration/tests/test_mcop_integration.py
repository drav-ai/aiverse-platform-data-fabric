"""
MCOP Integration Tests for Data Fabric Domain.

Tests verify:
1. Registry cards load successfully
2. Capabilities are discoverable
3. Intents map to execution units correctly
4. Feedback signals emit properly
5. Domain is fully removable
"""

import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest


# Mock implementations for testing without live MCOP
class MockAssetRegistryClient:
    """Mock Asset Registry for testing."""

    def __init__(self):
        self.registered: dict[str, UUID] = {}
        self.call_log: list[dict[str, Any]] = []

    async def register_capability(
        self,
        name: str,
        version: str,
        domain: str,
        capability_type: str,
        tags: list[str],
        description: str,
        input_contract: dict[str, Any],
        output_contract: dict[str, Any],
        consumer_intents: list[str],
        metadata: dict[str, Any],
    ) -> UUID:
        card_id = uuid4()
        self.registered[name] = card_id
        self.call_log.append({
            "action": "register",
            "name": name,
            "card_id": card_id,
        })
        return card_id

    async def unregister_capability(self, card_id: UUID) -> bool:
        for name, cid in list(self.registered.items()):
            if cid == card_id:
                del self.registered[name]
                self.call_log.append({
                    "action": "unregister",
                    "card_id": card_id,
                })
                return True
        return False

    async def get_capabilities_by_domain(self, domain: str) -> list[dict[str, Any]]:
        return [{"name": name, "id": str(cid)} for name, cid in self.registered.items()]


class MockObservabilitySpine:
    """Mock Observability Spine for testing."""

    def __init__(self):
        self.metrics: list[dict] = []
        self.outcomes: list[dict] = []
        self.advisors: list[dict] = []

    async def emit_metric(
        self,
        metric_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        timestamp,
    ) -> bool:
        self.metrics.append({"name": metric_name, "value": value})
        return True

    async def emit_outcome(
        self,
        outcome_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        timestamp,
    ) -> bool:
        self.outcomes.append({"name": outcome_name, "value": value})
        return True

    async def emit_advisor(
        self,
        advisor_name: str,
        value: dict[str, Any],
        tenant_context: dict[str, str],
        intended_consumer: str,
        timestamp,
    ) -> bool:
        self.advisors.append({
            "name": advisor_name,
            "value": value,
            "consumer": intended_consumer,
        })
        return True


@pytest.fixture
def base_path():
    """Get base path to aiverse-platform-data-fabric."""
    return Path(__file__).parent.parent.parent


@pytest.fixture
def mock_registry_client():
    """Create mock registry client."""
    return MockAssetRegistryClient()


@pytest.fixture
def mock_observability_spine():
    """Create mock observability spine."""
    return MockObservabilitySpine()


@pytest.fixture
def tenant_context():
    """Create test tenant context."""
    return {
        "organization_id": str(uuid4()),
        "workspace_id": str(uuid4()),
        "user_id": str(uuid4()),
    }


class TestRegistryCardLoading:
    """Tests for registry card loading."""

    def test_discover_all_cards(self, base_path):
        """Test that all 22 registry cards are discovered."""
        from ..mcop.registry_loader import RegistryCardLoader

        loader = RegistryCardLoader(
            registry_client=MockAssetRegistryClient(),
            base_path=base_path,
        )
        cards = loader.discover_cards()

        assert len(cards) == 22, f"Expected 22 cards, found {len(cards)}"

    def test_card_schema_validity(self, base_path):
        """Test that all cards have required fields."""
        cards_path = base_path / "registry_cards"

        required_fields = ["metadata", "capability", "input_contract", "output_contract"]

        for card_file in cards_path.glob("*.json"):
            with open(card_file) as f:
                data = json.load(f)

            for field in required_fields:
                assert field in data, f"{card_file.name} missing field: {field}"

            assert "name" in data["metadata"], f"{card_file.name} missing metadata.name"
            assert "type" in data["capability"], f"{card_file.name} missing capability.type"

    @pytest.mark.asyncio
    async def test_load_all_cards(self, base_path, mock_registry_client):
        """Test loading all cards into registry."""
        from ..mcop.registry_loader import RegistryCardLoader

        loader = RegistryCardLoader(
            registry_client=mock_registry_client,
            base_path=base_path,
        )
        results = await loader.load_all()

        assert len(results) == 22
        assert all(isinstance(v, UUID) for v in results.values())
        assert mock_registry_client.registered == results


class TestCapabilityProvider:
    """Tests for capability provider."""

    def test_all_execution_units_have_capabilities(self):
        """Test that all 22 EUs have capability profiles."""
        from ..mcop.capability_provider import EXECUTION_UNIT_CAPABILITIES

        assert len(EXECUTION_UNIT_CAPABILITIES) == 22

        expected_units = [
            "DataAssetRegistrar", "ConnectionProbe", "SchemaIntrospector",
            "DataExtractor", "DataWriter", "TransformExecutor",
            "DataJoiner", "AggregationComputer", "FeatureComputer",
            "FeatureStoreWriter", "FeatureRetriever", "DataProfiler",
            "SchemaValidator", "DataCommitter", "BranchCreator",
            "MergeComputer", "DataReplicator", "LocalitySignalGenerator",
            "LabelTaskCreator", "LabelRecorder", "LineageEdgeWriter",
            "QualityGateEvaluator",
        ]

        for unit in expected_units:
            assert unit in EXECUTION_UNIT_CAPABILITIES, f"Missing capability for {unit}"

    def test_capability_profiles_are_valid(self):
        """Test that capability profiles have required fields."""
        from ..mcop.capability_provider import EXECUTION_UNIT_CAPABILITIES

        for name, profile in EXECUTION_UNIT_CAPABILITIES.items():
            assert profile.capability_type, f"{name} missing capability_type"
            assert profile.compute_class, f"{name} missing compute_class"
            assert profile.memory_requirements, f"{name} missing memory_requirements"
            assert "stateless" in profile.tags, f"{name} must have 'stateless' tag"


class TestIntentHandler:
    """Tests for intent handler."""

    def test_all_adr_intents_supported(self):
        """Test that all 15 ADR intents are supported."""
        from ..mcop.intent_handler import DataFabricIntentHandler

        handler = DataFabricIntentHandler()

        expected_intents = [
            "RegisterDataAsset", "IngestData", "TransformData",
            "MaterializeFeatures", "RetrieveFeatures", "ProfileData",
            "CommitDataVersion", "BranchDataset", "MergeDataBranches",
            "CreateLabelTask", "TestConnection", "DiscoverSchema",
            "ReplicateData", "QueryLocality", "ValidateSchema",
        ]

        for intent in expected_intents:
            assert handler.is_supported_intent(intent), f"Intent not supported: {intent}"

        assert handler.get_intent_count() == 15

    def test_intent_to_eu_mapping(self):
        """Test that intents map to correct execution units."""
        from ..mcop.intent_handler import INTENT_TO_EXECUTION_UNITS

        # Verify mappings per ADR Section 4
        assert len(INTENT_TO_EXECUTION_UNITS["IngestData"]) == 3
        assert INTENT_TO_EXECUTION_UNITS["IngestData"][0].name == "DataExtractor"

        assert len(INTENT_TO_EXECUTION_UNITS["RegisterDataAsset"]) == 1
        assert INTENT_TO_EXECUTION_UNITS["RegisterDataAsset"][0].name == "DataAssetRegistrar"

        assert len(INTENT_TO_EXECUTION_UNITS["TransformData"]) == 3
        assert INTENT_TO_EXECUTION_UNITS["TransformData"][0].name == "TransformExecutor"

    @pytest.mark.asyncio
    async def test_handle_intent_returns_decomposition(self):
        """Test that handling an intent returns correct decomposition."""
        from ..mcop.intent_handler import DataFabricIntentHandler

        handler = DataFabricIntentHandler()
        result = await handler.handle_intent(
            intent_id=uuid4(),
            intent_type="IngestData",
            intent_params={},
        )

        assert result["success"] is True
        assert result["intent_type"] == "IngestData"
        assert result["domain"] == "data-fabric"
        assert result["unit_count"] == 3
        assert len(result["execution_units"]) == 3


class TestFeedbackSignals:
    """Tests for feedback signal integration."""

    def test_all_signals_loadable(self, base_path):
        """Test that all feedback signals load correctly."""
        from ..observability.signal_registry import FeedbackSignalRegistry

        registry = FeedbackSignalRegistry(base_path=base_path)
        registry.load()

        counts = registry.get_signal_count()

        assert counts["metrics"] == 4, f"Expected 4 metrics, got {counts['metrics']}"
        assert counts["outcomes"] == 3, f"Expected 3 outcomes, got {counts['outcomes']}"
        assert counts["advisors"] == 3, f"Expected 3 advisors, got {counts['advisors']}"
        assert counts["total"] == 10

    def test_signal_schema_validity(self, base_path):
        """Test that all signals have valid schemas."""
        signals_path = base_path / "feedback_signals"

        required_fields = ["metadata", "signal_type", "emission_trigger", "schema"]

        for signal_file in signals_path.glob("*.json"):
            with open(signal_file) as f:
                data = json.load(f)

            for field in required_fields:
                assert field in data, f"{signal_file.name} missing field: {field}"

            assert data["signal_type"] in ["metric", "outcome", "advisor"], \
                f"{signal_file.name} invalid signal_type"

    @pytest.mark.asyncio
    async def test_emit_metric(self, base_path, mock_observability_spine, tenant_context):
        """Test emitting a metric signal."""
        from ..observability.signal_emitter import FeedbackSignalEmitter
        from ..observability.signal_registry import FeedbackSignalRegistry

        registry = FeedbackSignalRegistry(base_path=base_path)
        emitter = FeedbackSignalEmitter(
            observability_spine=mock_observability_spine,
            signal_registry=registry,
        )

        result = await emitter.emit_metric(
            metric_name="DataIngestionVolume",
            intent_id=uuid4(),
            tenant_context=tenant_context,
            values={"bytes_ingested": 1024, "rows_ingested": 100},
        )

        assert result.success is True
        assert result.signal_type == "metric"
        assert len(mock_observability_spine.metrics) == 1

    @pytest.mark.asyncio
    async def test_emit_outcome(self, base_path, mock_observability_spine, tenant_context):
        """Test emitting an outcome signal."""
        from ..observability.signal_emitter import FeedbackSignalEmitter
        from ..observability.signal_registry import FeedbackSignalRegistry

        registry = FeedbackSignalRegistry(base_path=base_path)
        emitter = FeedbackSignalEmitter(
            observability_spine=mock_observability_spine,
            signal_registry=registry,
        )

        result = await emitter.emit_outcome(
            outcome_name="DataQualityGateOutcome",
            intent_id=uuid4(),
            tenant_context=tenant_context,
            values={"gate_result": "pass", "metric_values": {}},
        )

        assert result.success is True
        assert result.signal_type == "outcome"
        assert len(mock_observability_spine.outcomes) == 1


class TestDomainRemoval:
    """Tests for domain removal validation."""

    @pytest.mark.asyncio
    async def test_unload_all_cards(self, base_path, mock_registry_client):
        """Test that all cards can be unloaded."""
        from ..mcop.registry_loader import RegistryCardLoader

        loader = RegistryCardLoader(
            registry_client=mock_registry_client,
            base_path=base_path,
        )

        # Load all cards
        await loader.load_all()
        assert loader.get_card_count() == 22

        # Unload all cards
        results = await loader.unload_all()
        assert all(results.values()), "Some cards failed to unload"
        assert loader.get_card_count() == 0

    def test_no_cross_domain_imports(self, base_path):
        """Test that no execution units import from other domains."""
        forbidden_imports = [
            "aiverse_platform_training",
            "aiverse_platform_agent",
            "aiverse_platform_model",
            "aiverse_platform_ops",
        ]

        eu_path = base_path / "execution_units"
        for py_file in eu_path.glob("*.py"):
            content = py_file.read_text()
            for forbidden in forbidden_imports:
                assert forbidden not in content, \
                    f"{py_file.name} imports forbidden domain: {forbidden}"

    def test_no_kubernetes_imports(self, base_path):
        """Test that no execution units import Kubernetes directly."""
        forbidden_imports = ["import kubernetes", "from kubernetes"]

        eu_path = base_path / "execution_units"
        for py_file in eu_path.glob("*.py"):
            content = py_file.read_text()
            for forbidden in forbidden_imports:
                assert forbidden not in content, \
                    f"{py_file.name} has forbidden K8s import"

    def test_all_eus_are_stateless(self, base_path):
        """Test that all EUs are stateless (use Protocol injection)."""
        eu_path = base_path / "execution_units"

        for py_file in eu_path.glob("eu_*.py"):
            content = py_file.read_text()

            # Must have Protocol imports
            assert "Protocol" in content, f"{py_file.name} must use Protocol for deps"

            # Must not have module-level state
            forbidden_patterns = [
                "_cache =",
                "_state =",
                "_session =",
                "global ",
            ]
            for pattern in forbidden_patterns:
                assert pattern not in content, \
                    f"{py_file.name} has forbidden state pattern: {pattern}"
