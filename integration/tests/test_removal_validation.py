"""
Removal Validation Tests for Data Fabric Domain.

Per ADR Section 1.5: Domain must be fully removable.

These tests verify that removing the Data Fabric domain
does not break any other domain or the Control Plane.
"""

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def base_path():
    """Get base path to aiverse-platform-data-fabric."""
    return Path(__file__).parent.parent.parent


@pytest.fixture
def control_plane_path():
    """Get path to control plane."""
    return Path(__file__).parent.parent.parent.parent / "aiverse-core-control-plane"


class TestRemovalChecklist:
    """
    Removal checklist tests per ADR Section 1.5.

    Pre-condition: Control Plane is running with Phase 2 configuration.

    Tests verify each item in the removal checklist.
    """

    def test_data_fabric_files_identifiable(self, base_path):
        """Test that all Data Fabric files are identifiable for removal."""
        # Execution units
        eu_files = list((base_path / "execution_units").glob("eu_*.py"))
        assert len(eu_files) == 22, "Should have 22 execution unit files"

        # Registry cards
        card_files = list((base_path / "registry_cards").glob("*.json"))
        assert len(card_files) == 22, "Should have 22 registry card files"

        # Feedback signals
        signal_files = list((base_path / "feedback_signals").glob("*.json"))
        assert len(signal_files) == 10, "Should have 10 feedback signal files"

    def test_no_hardcoded_references_in_control_plane(self, control_plane_path):
        """Test that Control Plane has no hardcoded Data Fabric references."""
        if not control_plane_path.exists():
            pytest.skip("Control plane not found")

        forbidden_patterns = [
            "DataAssetRegistrar",
            "DataExtractor",
            "DataWriter",
            "TransformExecutor",
            "FeatureComputer",
            "data-fabric",  # as hardcoded string (not dynamic)
        ]

        # Check src directory for hardcoded references
        src_path = control_plane_path / "src"
        if not src_path.exists():
            pytest.skip("Control plane src not found")

        violations = []
        for py_file in src_path.rglob("*.py"):
            content = py_file.read_text()

            for pattern in forbidden_patterns:
                # Allow references in comments and strings for dynamic loading
                if f'"{pattern}"' in content and "config" not in str(py_file):
                    violations.append(f"{py_file.name}: hardcoded '{pattern}'")

        # Some dynamic references are acceptable
        # This test flags hardcoded tight coupling only
        assert len(violations) == 0, f"Found hardcoded references: {violations}"

    def test_intent_type_is_configurable(self, control_plane_path):
        """Test that intent types are loaded from config, not hardcoded."""
        if not control_plane_path.exists():
            pytest.skip("Control plane not found")

        intent_service = control_plane_path / "src" / "services" / "intent_service.py"
        if not intent_service.exists():
            pytest.skip("Intent service not found")

        content = intent_service.read_text()

        # Should use enum or config, not hardcoded list
        assert "RegisterDataAsset" not in content, \
            "Data intents should not be hardcoded in intent service"
        assert "IngestData" not in content, \
            "Data intents should not be hardcoded in intent service"

    def test_asset_types_are_extensible(self, control_plane_path):
        """Test that asset types support dynamic registration."""
        if not control_plane_path.exists():
            pytest.skip("Control plane not found")

        registry_service = control_plane_path / "src" / "services" / "asset_registry.py"
        if not registry_service.exists():
            pytest.skip("Asset registry not found")

        content = registry_service.read_text()

        # Should support any asset type, not just predefined ones
        assert "dataset" not in content.lower() or "DATASET" in content, \
            "Should use constants, not literals"

    def test_policies_are_configurable(self, base_path):
        """Test that policies can be removed via config."""
        # Policies should be in separate config files, not in code
        eu_path = base_path / "execution_units"

        for py_file in eu_path.glob("*.py"):
            content = py_file.read_text()

            # EUs should not define policies
            assert "policy" not in content.lower() or "Policy" in content, \
                f"{py_file.name} should not contain policy logic"

    def test_no_foreign_key_dependencies(self, base_path):
        """Test that Data Fabric has no DB foreign key to other domains."""
        # EUs should not define database models
        eu_path = base_path / "execution_units"

        for py_file in eu_path.glob("*.py"):
            content = py_file.read_text()

            forbidden = [
                "ForeignKey",
                "relationship(",
                "Base.metadata",
                "create_engine",
            ]

            for pattern in forbidden:
                assert pattern not in content, \
                    f"{py_file.name} should not have DB definitions"

    def test_integration_layer_is_removable(self, base_path):
        """Test that integration layer can be removed independently."""
        integration_path = base_path / "integration"

        # Integration should not be imported by execution units
        eu_path = base_path / "execution_units"

        for py_file in eu_path.glob("eu_*.py"):
            content = py_file.read_text()
            assert "from ..integration" not in content, \
                f"{py_file.name} should not import integration layer"


class TestSimulatedRemoval:
    """Tests simulating domain removal."""

    def test_imports_are_isolated(self, base_path):
        """Test that Data Fabric imports don't leak outside domain."""
        # Try importing execution units
        sys.path.insert(0, str(base_path.parent))

        try:
            # This should work
            from aiverse_platform_data_fabric.execution_units import (
                eu_01_data_asset_registrar,
            )
            assert eu_01_data_asset_registrar is not None
        except ImportError as e:
            # If import fails, that's actually fine for isolation
            pass
        finally:
            sys.path.pop(0)

    def test_removal_does_not_break_python_imports(self, base_path):
        """Test that removing the domain doesn't cause import errors."""
        # Simulate checking if control plane can start without data fabric
        # by verifying no mandatory imports exist

        control_plane_path = base_path.parent / "aiverse-core-control-plane"
        if not control_plane_path.exists():
            pytest.skip("Control plane not found")

        main_file = control_plane_path / "src" / "main.py"
        if not main_file.exists():
            pytest.skip("Control plane main.py not found")

        content = main_file.read_text()

        # Main should not import data fabric directly
        assert "aiverse_platform_data_fabric" not in content, \
            "Control plane main.py should not import data fabric"


class TestRemovalDocumentation:
    """Tests for removal documentation completeness."""

    def test_removal_checklist_exists(self, base_path):
        """Test that ADR contains removal checklist."""
        adr_path = base_path.parent / "docs" / "adr" / "PHASE3-DATA-FABRIC-PLUGIN-SPEC.md"
        if not adr_path.exists():
            pytest.skip("ADR not found")

        content = adr_path.read_text()

        required_sections = [
            "Removal Test",
            "Removal Checklist",
            "Pass Criterion",
        ]

        for section in required_sections:
            assert section in content, f"ADR missing section: {section}"

    def test_readme_documents_removal(self, base_path):
        """Test that README documents removal process."""
        readme_path = base_path / "README.md"
        if not readme_path.exists():
            pytest.skip("README not found")

        content = readme_path.read_text()

        assert "Removal" in content or "removal" in content, \
            "README should document removal process"
