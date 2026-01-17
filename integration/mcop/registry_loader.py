"""
Registry Card Loader for Data Fabric Execution Units.

Loads all registry cards from registry_cards/ and registers them
with the Control Plane Asset Registry.

Per ADR: This loader is fully removable - removing it does not break MCOP.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4


@dataclass(frozen=True)
class RegistryCard:
    """Parsed registry card from JSON."""
    name: str
    version: str
    domain: str
    capability_type: str
    capability_tags: list[str]
    description: str
    input_contract: dict[str, Any]
    output_contract: dict[str, Any]
    consumer_intents: list[str]
    failure_modes: list[str]
    adr_reference: str


class AssetRegistryClient(Protocol):
    """Protocol for Asset Registry interaction."""

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
        """Register a capability card. Returns card ID."""
        ...

    async def unregister_capability(self, card_id: UUID) -> bool:
        """Unregister a capability card."""
        ...

    async def get_capabilities_by_domain(self, domain: str) -> list[dict[str, Any]]:
        """Get all capabilities for a domain."""
        ...


class RegistryCardLoader:
    """
    Loads Data Fabric registry cards into MCOP Registry.

    This loader:
    1. Reads all JSON cards from registry_cards/
    2. Validates card schema
    3. Registers each card with the Asset Registry
    4. Tracks registered cards for removal
    """

    REGISTRY_CARDS_DIR = "registry_cards"
    DOMAIN = "data-fabric"

    def __init__(
        self,
        registry_client: AssetRegistryClient,
        base_path: Path | None = None,
    ):
        self._registry_client = registry_client
        self._base_path = base_path or Path(__file__).parent.parent.parent
        self._registered_cards: dict[str, UUID] = {}

    def _get_cards_path(self) -> Path:
        """Get path to registry cards directory."""
        return self._base_path / self.REGISTRY_CARDS_DIR

    def _load_card_from_file(self, card_path: Path) -> RegistryCard:
        """Load and parse a single registry card."""
        with open(card_path) as f:
            data = json.load(f)

        metadata = data.get("metadata", {})
        capability = data.get("capability", {})

        return RegistryCard(
            name=metadata.get("name", ""),
            version=metadata.get("version", "1.0.0"),
            domain=metadata.get("domain", self.DOMAIN),
            capability_type=capability.get("type", ""),
            capability_tags=capability.get("tags", []),
            description=capability.get("description", ""),
            input_contract=data.get("input_contract", {}),
            output_contract=data.get("output_contract", {}),
            consumer_intents=data.get("consumer_intents", []),
            failure_modes=data.get("failure_modes", []),
            adr_reference=metadata.get("adr_reference", ""),
        )

    def discover_cards(self) -> list[RegistryCard]:
        """Discover all registry cards in the cards directory."""
        cards_path = self._get_cards_path()
        if not cards_path.exists():
            return []

        cards = []
        for card_file in sorted(cards_path.glob("*.json")):
            try:
                card = self._load_card_from_file(card_file)
                cards.append(card)
            except (json.JSONDecodeError, KeyError) as e:
                # Log error but continue - don't fail entire load
                print(f"Warning: Failed to load card {card_file}: {e}")

        return cards

    async def load_all(self) -> dict[str, UUID]:
        """
        Load all registry cards into MCOP Registry.

        Returns dict of card_name -> card_id for all registered cards.
        """
        cards = self.discover_cards()
        results = {}

        for card in cards:
            try:
                card_id = await self._registry_client.register_capability(
                    name=card.name,
                    version=card.version,
                    domain=card.domain,
                    capability_type=card.capability_type,
                    tags=card.capability_tags,
                    description=card.description,
                    input_contract=card.input_contract,
                    output_contract=card.output_contract,
                    consumer_intents=card.consumer_intents,
                    metadata={
                        "failure_modes": card.failure_modes,
                        "adr_reference": card.adr_reference,
                        "registered_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                results[card.name] = card_id
                self._registered_cards[card.name] = card_id
            except Exception as e:
                # Log error but continue
                print(f"Warning: Failed to register card {card.name}: {e}")

        return results

    async def unload_all(self) -> dict[str, bool]:
        """
        Unload all registered cards from MCOP Registry.

        Returns dict of card_name -> success for all unregistered cards.
        Used for removal validation testing.
        """
        results = {}

        for card_name, card_id in list(self._registered_cards.items()):
            try:
                success = await self._registry_client.unregister_capability(card_id)
                results[card_name] = success
                if success:
                    del self._registered_cards[card_name]
            except Exception as e:
                print(f"Warning: Failed to unregister card {card_name}: {e}")
                results[card_name] = False

        return results

    def get_registered_cards(self) -> dict[str, UUID]:
        """Get all currently registered cards."""
        return dict(self._registered_cards)

    def get_card_count(self) -> int:
        """Get count of registered cards."""
        return len(self._registered_cards)
