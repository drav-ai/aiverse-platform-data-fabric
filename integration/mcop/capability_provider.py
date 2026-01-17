"""
Capability Provider for Data Fabric Domain.

Provides capability information to MCOP for scheduling decisions.

Per ADR: Capabilities are declarative - no execution logic here.
"""

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID


@dataclass(frozen=True)
class CapabilityProfile:
    """Capability profile for MCOP scheduling."""
    capability_type: str
    compute_class: str
    memory_requirements: str
    io_pattern: str
    tags: list[str]


@dataclass(frozen=True)
class LocalitySignal:
    """Data locality signal for MCOP placement."""
    asset_ref: str
    environment_id: str
    locality_type: str
    transfer_cost: float
    confidence: float


class MCOPScheduler(Protocol):
    """Protocol for MCOP scheduling interaction."""

    async def provide_capability(
        self,
        execution_unit_name: str,
        capability_profile: CapabilityProfile,
    ) -> bool:
        """Provide capability information to MCOP."""
        ...

    async def provide_locality_signal(
        self,
        intent_ref: UUID,
        signals: list[LocalitySignal],
    ) -> bool:
        """Provide locality signals to MCOP for placement."""
        ...


# Capability profiles for each execution unit type
EXECUTION_UNIT_CAPABILITIES: dict[str, CapabilityProfile] = {
    "DataAssetRegistrar": CapabilityProfile(
        capability_type="data-registration",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="write-registry",
        tags=["registry", "metadata", "stateless"],
    ),
    "ConnectionProbe": CapabilityProfile(
        capability_type="connection-testing",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="network-probe",
        tags=["connection", "health", "stateless"],
    ),
    "SchemaIntrospector": CapabilityProfile(
        capability_type="schema-discovery",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="read-external",
        tags=["schema", "discovery", "stateless"],
    ),
    "DataExtractor": CapabilityProfile(
        capability_type="data-extraction",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-external-write-staging",
        tags=["extraction", "ingestion", "stateless"],
    ),
    "DataWriter": CapabilityProfile(
        capability_type="data-writing",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-staging-write-dataset",
        tags=["write", "persistence", "stateless"],
    ),
    "TransformExecutor": CapabilityProfile(
        capability_type="data-transformation",
        compute_class="cpu-large",
        memory_requirements="high",
        io_pattern="read-process-write",
        tags=["transform", "processing", "stateless"],
    ),
    "DataJoiner": CapabilityProfile(
        capability_type="data-joining",
        compute_class="cpu-large",
        memory_requirements="high",
        io_pattern="read-multi-write",
        tags=["join", "merge", "stateless"],
    ),
    "AggregationComputer": CapabilityProfile(
        capability_type="data-aggregation",
        compute_class="cpu-medium",
        memory_requirements="high",
        io_pattern="read-aggregate-write",
        tags=["aggregation", "analytics", "stateless"],
    ),
    "FeatureComputer": CapabilityProfile(
        capability_type="feature-computation",
        compute_class="cpu-large",
        memory_requirements="high",
        io_pattern="read-compute-write",
        tags=["feature", "ml", "stateless"],
    ),
    "FeatureStoreWriter": CapabilityProfile(
        capability_type="feature-storage",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-staging-write-store",
        tags=["feature", "store", "stateless"],
    ),
    "FeatureRetriever": CapabilityProfile(
        capability_type="feature-retrieval",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="read-store",
        tags=["feature", "retrieval", "stateless"],
    ),
    "DataProfiler": CapabilityProfile(
        capability_type="data-profiling",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-analyze",
        tags=["profiling", "quality", "stateless"],
    ),
    "SchemaValidator": CapabilityProfile(
        capability_type="schema-validation",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="read-validate",
        tags=["schema", "validation", "stateless"],
    ),
    "DataCommitter": CapabilityProfile(
        capability_type="data-versioning",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="read-commit",
        tags=["commit", "versioning", "stateless"],
    ),
    "BranchCreator": CapabilityProfile(
        capability_type="data-branching",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="write-registry",
        tags=["branch", "versioning", "stateless"],
    ),
    "MergeComputer": CapabilityProfile(
        capability_type="data-merging",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-multi-compute",
        tags=["merge", "versioning", "stateless"],
    ),
    "DataReplicator": CapabilityProfile(
        capability_type="data-replication",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-write-remote",
        tags=["replication", "locality", "stateless"],
    ),
    "LocalitySignalGenerator": CapabilityProfile(
        capability_type="locality-signaling",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="read-probe",
        tags=["locality", "scheduling", "stateless"],
    ),
    "LabelTaskCreator": CapabilityProfile(
        capability_type="labeling-task",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="write-registry",
        tags=["labeling", "task", "stateless"],
    ),
    "LabelRecorder": CapabilityProfile(
        capability_type="label-recording",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="write-store",
        tags=["labeling", "annotation", "stateless"],
    ),
    "LineageEdgeWriter": CapabilityProfile(
        capability_type="lineage-recording",
        compute_class="cpu-small",
        memory_requirements="low",
        io_pattern="write-registry",
        tags=["lineage", "provenance", "stateless"],
    ),
    "QualityGateEvaluator": CapabilityProfile(
        capability_type="quality-evaluation",
        compute_class="cpu-medium",
        memory_requirements="medium",
        io_pattern="read-evaluate",
        tags=["quality", "gate", "stateless"],
    ),
}


class DataFabricCapabilityProvider:
    """
    Provides Data Fabric capabilities to MCOP.

    This provider:
    1. Maps execution units to capability profiles
    2. Provides locality signals for data-aware scheduling
    3. Does NOT execute any logic - purely declarative
    """

    DOMAIN = "data-fabric"

    def __init__(self, mcop_scheduler: MCOPScheduler | None = None):
        self._mcop_scheduler = mcop_scheduler
        self._provided_capabilities: set[str] = set()

    async def provide_all_capabilities(self) -> dict[str, bool]:
        """
        Provide all Data Fabric capabilities to MCOP.

        Returns dict of execution_unit_name -> success.
        """
        if not self._mcop_scheduler:
            # Return mock success if no scheduler configured
            return {name: True for name in EXECUTION_UNIT_CAPABILITIES}

        results = {}
        for name, profile in EXECUTION_UNIT_CAPABILITIES.items():
            try:
                success = await self._mcop_scheduler.provide_capability(name, profile)
                results[name] = success
                if success:
                    self._provided_capabilities.add(name)
            except Exception as e:
                print(f"Warning: Failed to provide capability {name}: {e}")
                results[name] = False

        return results

    async def provide_locality_signals(
        self,
        intent_ref: UUID,
        asset_ref: str,
        signals: list[LocalitySignal],
    ) -> bool:
        """
        Provide locality signals to MCOP for intent scheduling.

        Per ADR: Signals influence placement but don't determine it.
        """
        if not self._mcop_scheduler:
            return True

        try:
            return await self._mcop_scheduler.provide_locality_signal(
                intent_ref=intent_ref,
                signals=signals,
            )
        except Exception as e:
            print(f"Warning: Failed to provide locality signals: {e}")
            return False

    def get_capability_profile(self, execution_unit_name: str) -> CapabilityProfile | None:
        """Get capability profile for an execution unit."""
        return EXECUTION_UNIT_CAPABILITIES.get(execution_unit_name)

    def get_provided_capabilities(self) -> set[str]:
        """Get set of capabilities currently provided to MCOP."""
        return self._provided_capabilities.copy()

    def get_capability_count(self) -> int:
        """Get total count of available capabilities."""
        return len(EXECUTION_UNIT_CAPABILITIES)
