"""MCOP Integration for Data Fabric Domain."""

from .registry_loader import RegistryCardLoader
from .capability_provider import DataFabricCapabilityProvider
from .intent_handler import DataFabricIntentHandler

__all__ = [
    "RegistryCardLoader",
    "DataFabricCapabilityProvider",
    "DataFabricIntentHandler",
]
