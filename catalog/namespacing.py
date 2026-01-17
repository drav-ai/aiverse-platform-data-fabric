"""
Data Catalog Namespacing Configuration

Defines namespace hierarchy and isolation rules for multi-tenant data catalog.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from uuid import uuid4


class NamespaceLevel(Enum):
    """Hierarchical namespace levels."""
    ORGANIZATION = "organization"
    WORKSPACE = "workspace"
    PROJECT = "project"
    DATASET = "dataset"


@dataclass
class NamespaceConfig:
    """Namespace configuration for data catalog."""
    
    # Namespace hierarchy
    hierarchy: List[NamespaceLevel] = field(default_factory=lambda: [
        NamespaceLevel.ORGANIZATION,
        NamespaceLevel.WORKSPACE,
        NamespaceLevel.PROJECT,
        NamespaceLevel.DATASET
    ])
    
    # Isolation mode
    isolation_mode: str = "strict"  # strict, shared, hybrid
    
    # Cross-namespace access rules
    allow_cross_workspace: bool = False
    allow_cross_organization: bool = False
    
    # Namespace patterns
    naming_pattern: str = "{org}/{workspace}/{project}/{dataset}"
    
    def validate_namespace(self, namespace: str) -> bool:
        """Validate namespace format."""
        parts = namespace.split("/")
        return len(parts) <= len(self.hierarchy)
    
    def get_isolation_scope(self, tenant_context: Dict[str, str]) -> str:
        """Get isolation scope for tenant."""
        if self.isolation_mode == "strict":
            return f"{tenant_context['organization_id']}/{tenant_context['workspace_id']}"
        elif self.isolation_mode == "shared":
            return tenant_context['organization_id']
        else:
            return f"{tenant_context['organization_id']}"


@dataclass
class CatalogEntry:
    """A single entry in the data catalog."""
    
    entry_id: str = field(default_factory=lambda: str(uuid4()))
    namespace: str = ""
    entry_type: str = ""  # dataset, feature_set, connection, schema
    name: str = ""
    version: str = "1.0.0"
    
    # Ownership
    owner_org: str = ""
    owner_workspace: str = ""
    owner_user: str = ""
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    schema_ref: Optional[str] = None
    lineage_refs: List[str] = field(default_factory=list)
    
    # Status
    status: str = "active"  # active, deprecated, archived
    created_at: str = ""
    updated_at: str = ""
    
    def get_fully_qualified_name(self) -> str:
        """Get FQN for catalog entry."""
        return f"{self.namespace}/{self.name}@{self.version}"


class CatalogNamespaceManager:
    """Manages namespace operations for data catalog."""
    
    def __init__(self, config: NamespaceConfig):
        self.config = config
        self._namespaces: Dict[str, Dict[str, Any]] = {}
        
    def create_namespace(
        self,
        org_id: str,
        workspace_id: str,
        project_id: Optional[str] = None
    ) -> str:
        """Create a new namespace."""
        if project_id:
            ns = f"{org_id}/{workspace_id}/{project_id}"
        else:
            ns = f"{org_id}/{workspace_id}"
            
        self._namespaces[ns] = {
            "created_at": str(uuid4())[:10],
            "entries": []
        }
        return ns
        
    def register_entry(
        self,
        namespace: str,
        entry: CatalogEntry
    ) -> CatalogEntry:
        """Register an entry in namespace."""
        if namespace not in self._namespaces:
            raise ValueError(f"Namespace not found: {namespace}")
            
        entry.namespace = namespace
        self._namespaces[namespace]["entries"].append(entry.entry_id)
        return entry
        
    def can_access(
        self,
        namespace: str,
        tenant_context: Dict[str, str]
    ) -> bool:
        """Check if tenant can access namespace."""
        ns_parts = namespace.split("/")
        
        if len(ns_parts) >= 1:
            if ns_parts[0] != tenant_context["organization_id"]:
                if not self.config.allow_cross_organization:
                    return False
                    
        if len(ns_parts) >= 2:
            if ns_parts[1] != tenant_context["workspace_id"]:
                if not self.config.allow_cross_workspace:
                    return False
                    
        return True
        
    def list_accessible_namespaces(
        self,
        tenant_context: Dict[str, str]
    ) -> List[str]:
        """List namespaces accessible to tenant."""
        accessible = []
        for ns in self._namespaces:
            if self.can_access(ns, tenant_context):
                accessible.append(ns)
        return accessible


# Default configuration
DEFAULT_NAMESPACE_CONFIG = NamespaceConfig(
    hierarchy=[
        NamespaceLevel.ORGANIZATION,
        NamespaceLevel.WORKSPACE,
        NamespaceLevel.PROJECT,
        NamespaceLevel.DATASET
    ],
    isolation_mode="strict",
    allow_cross_workspace=False,
    allow_cross_organization=False,
    naming_pattern="{org}/{workspace}/{project}/{dataset}"
)
