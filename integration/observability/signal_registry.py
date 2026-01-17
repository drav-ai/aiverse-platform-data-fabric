"""
Feedback Signal Registry for Data Fabric Domain.

Loads feedback signal definitions from feedback_signals/ and provides
metadata for signal emission and consumption.

Per ADR: Signals are declarative metadata - no execution logic.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SignalDefinition:
    """Parsed signal definition from JSON."""
    name: str
    version: str
    signal_type: str  # metric, outcome, advisor
    description: str
    emission_trigger: dict[str, Any]
    schema: dict[str, Any]
    intended_consumers: list[dict[str, str]]
    domain: str = "data-fabric"


@dataclass
class SignalRegistry:
    """Registry of feedback signal definitions."""
    metrics: dict[str, SignalDefinition] = field(default_factory=dict)
    outcomes: dict[str, SignalDefinition] = field(default_factory=dict)
    advisors: dict[str, SignalDefinition] = field(default_factory=dict)

    def add(self, signal: SignalDefinition) -> None:
        """Add a signal to the appropriate category."""
        if signal.signal_type == "metric":
            self.metrics[signal.name] = signal
        elif signal.signal_type == "outcome":
            self.outcomes[signal.name] = signal
        elif signal.signal_type == "advisor":
            self.advisors[signal.name] = signal

    def get(self, name: str) -> SignalDefinition | None:
        """Get a signal by name from any category."""
        return (
            self.metrics.get(name)
            or self.outcomes.get(name)
            or self.advisors.get(name)
        )

    def get_all(self) -> list[SignalDefinition]:
        """Get all signals."""
        return list(self.metrics.values()) + list(self.outcomes.values()) + list(self.advisors.values())

    def get_by_type(self, signal_type: str) -> list[SignalDefinition]:
        """Get signals by type."""
        if signal_type == "metric":
            return list(self.metrics.values())
        elif signal_type == "outcome":
            return list(self.outcomes.values())
        elif signal_type == "advisor":
            return list(self.advisors.values())
        return []


class FeedbackSignalRegistry:
    """
    Loads and provides access to Data Fabric feedback signal definitions.

    This registry:
    1. Reads all JSON signals from feedback_signals/
    2. Categorizes by type (metric, outcome, advisor)
    3. Provides signal metadata for emission and consumption
    """

    FEEDBACK_SIGNALS_DIR = "feedback_signals"
    DOMAIN = "data-fabric"

    def __init__(self, base_path: Path | None = None):
        self._base_path = base_path or Path(__file__).parent.parent.parent
        self._registry = SignalRegistry()
        self._loaded = False

    def _get_signals_path(self) -> Path:
        """Get path to feedback signals directory."""
        return self._base_path / self.FEEDBACK_SIGNALS_DIR

    def _load_signal_from_file(self, signal_path: Path) -> SignalDefinition:
        """Load and parse a single signal definition."""
        with open(signal_path) as f:
            data = json.load(f)

        metadata = data.get("metadata", {})

        return SignalDefinition(
            name=metadata.get("name", ""),
            version=metadata.get("version", "1.0.0"),
            signal_type=data.get("signal_type", "metric"),
            description=data.get("description", ""),
            emission_trigger=data.get("emission_trigger", {}),
            schema=data.get("schema", {}),
            intended_consumers=data.get("intended_consumers", []),
            domain=metadata.get("domain", self.DOMAIN),
        )

    def load(self) -> SignalRegistry:
        """
        Load all feedback signal definitions.

        Returns the populated signal registry.
        """
        signals_path = self._get_signals_path()
        if not signals_path.exists():
            return self._registry

        for signal_file in sorted(signals_path.glob("*.json")):
            try:
                signal = self._load_signal_from_file(signal_file)
                self._registry.add(signal)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Warning: Failed to load signal {signal_file}: {e}")

        self._loaded = True
        return self._registry

    def get_registry(self) -> SignalRegistry:
        """Get the signal registry, loading if necessary."""
        if not self._loaded:
            self.load()
        return self._registry

    def get_signal(self, name: str) -> SignalDefinition | None:
        """Get a specific signal definition by name."""
        return self.get_registry().get(name)

    def get_signals_for_execution_unit(self, eu_name: str) -> list[SignalDefinition]:
        """Get all signals triggered by an execution unit."""
        registry = self.get_registry()
        matching = []

        for signal in registry.get_all():
            trigger_units = signal.emission_trigger.get("execution_units", [])
            if eu_name in trigger_units:
                matching.append(signal)

        return matching

    def get_metrics(self) -> list[SignalDefinition]:
        """Get all metric signals."""
        return self.get_registry().get_by_type("metric")

    def get_outcomes(self) -> list[SignalDefinition]:
        """Get all outcome signals."""
        return self.get_registry().get_by_type("outcome")

    def get_advisors(self) -> list[SignalDefinition]:
        """Get all advisor signals."""
        return self.get_registry().get_by_type("advisor")

    def get_signal_count(self) -> dict[str, int]:
        """Get count of signals by type."""
        registry = self.get_registry()
        return {
            "metrics": len(registry.metrics),
            "outcomes": len(registry.outcomes),
            "advisors": len(registry.advisors),
            "total": len(registry.get_all()),
        }
