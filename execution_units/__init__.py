"""
Data Fabric Execution Units

Per ADR: PHASE3-DATA-FABRIC-PLUGIN-SPEC.md

All units are:
- Stateless
- MCOP-scheduled
- Single capability
- No orchestration
- Terminable at any time with no side effects
"""

from . import eu_01_data_asset_registrar
from . import eu_02_connection_probe
from . import eu_03_schema_introspector
from . import eu_04_data_extractor
from . import eu_05_data_writer
from . import eu_06_transform_executor
from . import eu_07_data_joiner
from . import eu_08_aggregation_computer
from . import eu_09_feature_computer
from . import eu_10_feature_store_writer
from . import eu_11_feature_retriever
from . import eu_12_data_profiler
from . import eu_13_schema_validator
from . import eu_14_data_committer
from . import eu_15_branch_creator
from . import eu_16_merge_computer
from . import eu_17_data_replicator
from . import eu_18_locality_signal_generator
from . import eu_19_label_task_creator
from . import eu_20_label_recorder
from . import eu_21_lineage_edge_writer
from . import eu_22_quality_gate_evaluator

__all__ = [
    "eu_01_data_asset_registrar",
    "eu_02_connection_probe",
    "eu_03_schema_introspector",
    "eu_04_data_extractor",
    "eu_05_data_writer",
    "eu_06_transform_executor",
    "eu_07_data_joiner",
    "eu_08_aggregation_computer",
    "eu_09_feature_computer",
    "eu_10_feature_store_writer",
    "eu_11_feature_retriever",
    "eu_12_data_profiler",
    "eu_13_schema_validator",
    "eu_14_data_committer",
    "eu_15_branch_creator",
    "eu_16_merge_computer",
    "eu_17_data_replicator",
    "eu_18_locality_signal_generator",
    "eu_19_label_task_creator",
    "eu_20_label_recorder",
    "eu_21_lineage_edge_writer",
    "eu_22_quality_gate_evaluator",
]
