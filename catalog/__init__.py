"""
Data Catalog Module

Provides namespacing, tagging, and drift control for data assets.
"""
from .namespacing import (
    NamespaceLevel,
    NamespaceConfig,
    CatalogEntry,
    CatalogNamespaceManager,
    DEFAULT_NAMESPACE_CONFIG,
)
from .tagging import (
    TagCategory,
    TagDefinition,
    TagInstance,
    TagSchema,
    STANDARD_TAG_DEFINITIONS,
    DEFAULT_TAG_SCHEMA,
    create_standard_tag_schema,
)

__all__ = [
    "NamespaceLevel",
    "NamespaceConfig", 
    "CatalogEntry",
    "CatalogNamespaceManager",
    "DEFAULT_NAMESPACE_CONFIG",
    "TagCategory",
    "TagDefinition",
    "TagInstance",
    "TagSchema",
    "STANDARD_TAG_DEFINITIONS",
    "DEFAULT_TAG_SCHEMA",
    "create_standard_tag_schema",
]
