"""
Data Catalog Tagging System

Defines tag schemas, validation, and governance rules.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
from enum import Enum
from datetime import datetime, timezone


class TagCategory(Enum):
    """Tag categories for classification."""
    CLASSIFICATION = "classification"      # PII, PHI, confidential
    DOMAIN = "domain"                      # finance, hr, sales
    QUALITY = "quality"                    # verified, experimental
    LIFECYCLE = "lifecycle"                # production, staging, dev
    COMPLIANCE = "compliance"              # gdpr, hipaa, sox
    OWNERSHIP = "ownership"                # team, cost_center
    TECHNICAL = "technical"                # format, compression


@dataclass
class TagDefinition:
    """Definition of a tag."""
    
    key: str
    category: TagCategory
    description: str = ""
    allowed_values: Optional[List[str]] = None  # None = freeform
    required: bool = False
    default_value: Optional[str] = None
    governance_level: str = "standard"  # standard, restricted, audit
    
    def validate_value(self, value: str) -> bool:
        """Validate tag value."""
        if self.allowed_values is None:
            return True
        return value in self.allowed_values


@dataclass
class TagInstance:
    """An applied tag instance."""
    
    key: str
    value: str
    applied_by: str = ""
    applied_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source: str = "manual"  # manual, automated, inherited


class TagSchema:
    """Schema for tag governance."""
    
    def __init__(self):
        self._definitions: Dict[str, TagDefinition] = {}
        self._required_tags: Set[str] = set()
        
    def register_tag(self, definition: TagDefinition) -> None:
        """Register a tag definition."""
        self._definitions[definition.key] = definition
        if definition.required:
            self._required_tags.add(definition.key)
            
    def validate_tags(self, tags: Dict[str, str]) -> Dict[str, Any]:
        """Validate a set of tags against schema."""
        result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check required tags
        for req_tag in self._required_tags:
            if req_tag not in tags:
                result["valid"] = False
                result["errors"].append(f"Missing required tag: {req_tag}")
                
        # Validate each tag
        for key, value in tags.items():
            if key in self._definitions:
                defn = self._definitions[key]
                if not defn.validate_value(value):
                    result["valid"] = False
                    result["errors"].append(
                        f"Invalid value '{value}' for tag '{key}'. "
                        f"Allowed: {defn.allowed_values}"
                    )
            else:
                result["warnings"].append(f"Unknown tag: {key}")
                
        return result
        
    def get_required_tags(self) -> List[str]:
        """Get list of required tags."""
        return list(self._required_tags)
        
    def get_tags_by_category(self, category: TagCategory) -> List[TagDefinition]:
        """Get tags by category."""
        return [d for d in self._definitions.values() if d.category == category]


# Standard tag definitions for Data Fabric
STANDARD_TAG_DEFINITIONS = [
    # Classification tags
    TagDefinition(
        key="data_classification",
        category=TagCategory.CLASSIFICATION,
        description="Data sensitivity classification",
        allowed_values=["public", "internal", "confidential", "restricted", "pii", "phi"],
        required=True,
        governance_level="audit"
    ),
    
    # Domain tags
    TagDefinition(
        key="business_domain",
        category=TagCategory.DOMAIN,
        description="Business domain ownership",
        allowed_values=["finance", "hr", "sales", "marketing", "operations", "product", "engineering"],
        required=True,
        governance_level="standard"
    ),
    
    # Quality tags
    TagDefinition(
        key="data_quality",
        category=TagCategory.QUALITY,
        description="Data quality tier",
        allowed_values=["gold", "silver", "bronze", "raw"],
        required=False,
        default_value="bronze",
        governance_level="standard"
    ),
    
    # Lifecycle tags
    TagDefinition(
        key="environment",
        category=TagCategory.LIFECYCLE,
        description="Environment tier",
        allowed_values=["production", "staging", "development", "sandbox"],
        required=True,
        governance_level="restricted"
    ),
    
    # Compliance tags
    TagDefinition(
        key="compliance_scope",
        category=TagCategory.COMPLIANCE,
        description="Compliance requirements",
        allowed_values=["gdpr", "hipaa", "sox", "pci", "none"],
        required=False,
        governance_level="audit"
    ),
    
    # Ownership tags
    TagDefinition(
        key="cost_center",
        category=TagCategory.OWNERSHIP,
        description="Cost allocation center",
        allowed_values=None,  # Freeform
        required=False,
        governance_level="standard"
    ),
    TagDefinition(
        key="owner_team",
        category=TagCategory.OWNERSHIP,
        description="Owning team",
        allowed_values=None,  # Freeform
        required=True,
        governance_level="standard"
    ),
    
    # Technical tags
    TagDefinition(
        key="storage_format",
        category=TagCategory.TECHNICAL,
        description="Data storage format",
        allowed_values=["parquet", "delta", "iceberg", "json", "csv", "avro"],
        required=False,
        governance_level="standard"
    ),
    TagDefinition(
        key="retention_days",
        category=TagCategory.TECHNICAL,
        description="Data retention period in days",
        allowed_values=None,  # Freeform numeric
        required=False,
        governance_level="restricted"
    ),
]


def create_standard_tag_schema() -> TagSchema:
    """Create standard tag schema with all definitions."""
    schema = TagSchema()
    for defn in STANDARD_TAG_DEFINITIONS:
        schema.register_tag(defn)
    return schema


# Default schema instance
DEFAULT_TAG_SCHEMA = create_standard_tag_schema()
