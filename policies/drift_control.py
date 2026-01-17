"""
Data Catalog Drift Control Policies

Detects and manages schema, data, and metadata drift in the catalog.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from datetime import datetime, timezone
from uuid import uuid4


class DriftType(Enum):
    """Types of drift that can be detected."""
    SCHEMA = "schema"           # Column additions, removals, type changes
    DATA = "data"               # Data distribution changes
    METADATA = "metadata"       # Tag, ownership, classification changes
    LINEAGE = "lineage"         # Upstream/downstream changes
    FRESHNESS = "freshness"     # Data staleness
    QUALITY = "quality"         # Quality metric degradation


class DriftSeverity(Enum):
    """Severity levels for drift detection."""
    INFO = "info"           # Informational, no action required
    WARNING = "warning"     # Attention needed, not blocking
    ERROR = "error"         # Action required, may block
    CRITICAL = "critical"   # Immediate action required, blocking


@dataclass
class DriftPolicy:
    """Policy for drift detection and response."""
    
    policy_id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    drift_type: DriftType = DriftType.SCHEMA
    
    # Detection configuration
    detection_enabled: bool = True
    detection_frequency: str = "hourly"  # realtime, hourly, daily, weekly
    
    # Thresholds
    warning_threshold: Optional[float] = None
    error_threshold: Optional[float] = None
    
    # Response actions
    auto_remediate: bool = False
    notify_owners: bool = True
    block_downstream: bool = False
    require_approval: bool = False
    
    # Scope
    apply_to_namespaces: List[str] = field(default_factory=list)  # Empty = all
    apply_to_tags: Dict[str, str] = field(default_factory=dict)  # Tag filters
    
    def should_apply(self, namespace: str, tags: Dict[str, str]) -> bool:
        """Check if policy applies to given scope."""
        # Namespace filter
        if self.apply_to_namespaces:
            if not any(namespace.startswith(ns) for ns in self.apply_to_namespaces):
                return False
                
        # Tag filter
        for key, value in self.apply_to_tags.items():
            if tags.get(key) != value:
                return False
                
        return True


@dataclass
class DriftEvent:
    """A detected drift event."""
    
    event_id: str = field(default_factory=lambda: str(uuid4()))
    drift_type: DriftType = DriftType.SCHEMA
    severity: DriftSeverity = DriftSeverity.WARNING
    
    # What drifted
    asset_id: str = ""
    asset_name: str = ""
    namespace: str = ""
    
    # Drift details
    previous_state: Dict[str, Any] = field(default_factory=dict)
    current_state: Dict[str, Any] = field(default_factory=dict)
    drift_description: str = ""
    
    # Timing
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # Status
    status: str = "open"  # open, acknowledged, resolved, ignored
    resolved_by: Optional[str] = None
    resolved_at: Optional[str] = None


class DriftDetector:
    """Detects drift in data catalog entries."""
    
    def __init__(self):
        self._policies: Dict[str, DriftPolicy] = {}
        self._events: List[DriftEvent] = []
        
    def register_policy(self, policy: DriftPolicy) -> None:
        """Register a drift policy."""
        self._policies[policy.policy_id] = policy
        
    def detect_schema_drift(
        self,
        asset_id: str,
        previous_schema: Dict[str, Any],
        current_schema: Dict[str, Any]
    ) -> Optional[DriftEvent]:
        """Detect schema drift between versions."""
        drift_details = []
        
        prev_cols = {c["name"]: c for c in previous_schema.get("columns", [])}
        curr_cols = {c["name"]: c for c in current_schema.get("columns", [])}
        
        # Added columns
        added = set(curr_cols.keys()) - set(prev_cols.keys())
        if added:
            drift_details.append(f"Added columns: {added}")
            
        # Removed columns
        removed = set(prev_cols.keys()) - set(curr_cols.keys())
        if removed:
            drift_details.append(f"Removed columns: {removed}")
            
        # Type changes
        for col_name in set(prev_cols.keys()) & set(curr_cols.keys()):
            if prev_cols[col_name].get("type") != curr_cols[col_name].get("type"):
                drift_details.append(
                    f"Type change for {col_name}: "
                    f"{prev_cols[col_name].get('type')} -> {curr_cols[col_name].get('type')}"
                )
                
        if not drift_details:
            return None
            
        severity = DriftSeverity.WARNING
        if removed:
            severity = DriftSeverity.ERROR
            
        return DriftEvent(
            drift_type=DriftType.SCHEMA,
            severity=severity,
            asset_id=asset_id,
            previous_state=previous_schema,
            current_state=current_schema,
            drift_description="; ".join(drift_details)
        )
        
    def detect_freshness_drift(
        self,
        asset_id: str,
        last_update: datetime,
        expected_frequency_hours: int = 24
    ) -> Optional[DriftEvent]:
        """Detect data freshness drift."""
        now = datetime.now(timezone.utc)
        age_hours = (now - last_update).total_seconds() / 3600
        
        if age_hours <= expected_frequency_hours:
            return None
            
        severity = DriftSeverity.WARNING
        if age_hours > expected_frequency_hours * 2:
            severity = DriftSeverity.ERROR
        if age_hours > expected_frequency_hours * 5:
            severity = DriftSeverity.CRITICAL
            
        return DriftEvent(
            drift_type=DriftType.FRESHNESS,
            severity=severity,
            asset_id=asset_id,
            previous_state={"expected_frequency_hours": expected_frequency_hours},
            current_state={"age_hours": age_hours},
            drift_description=f"Data is {age_hours:.1f} hours old, expected refresh every {expected_frequency_hours} hours"
        )
        
    def detect_quality_drift(
        self,
        asset_id: str,
        baseline_metrics: Dict[str, float],
        current_metrics: Dict[str, float],
        threshold_pct: float = 0.1
    ) -> Optional[DriftEvent]:
        """Detect data quality metric drift."""
        drift_details = []
        
        for metric, baseline in baseline_metrics.items():
            current = current_metrics.get(metric, 0)
            if baseline > 0:
                change_pct = abs(current - baseline) / baseline
                if change_pct > threshold_pct:
                    drift_details.append(
                        f"{metric}: {baseline:.2f} -> {current:.2f} ({change_pct*100:.1f}% change)"
                    )
                    
        if not drift_details:
            return None
            
        return DriftEvent(
            drift_type=DriftType.QUALITY,
            severity=DriftSeverity.WARNING,
            asset_id=asset_id,
            previous_state=baseline_metrics,
            current_state=current_metrics,
            drift_description="; ".join(drift_details)
        )
        
    def record_event(self, event: DriftEvent) -> None:
        """Record a drift event."""
        self._events.append(event)
        
    def get_open_events(self, asset_id: Optional[str] = None) -> List[DriftEvent]:
        """Get open drift events."""
        events = [e for e in self._events if e.status == "open"]
        if asset_id:
            events = [e for e in events if e.asset_id == asset_id]
        return events
        
    def resolve_event(self, event_id: str, resolved_by: str) -> None:
        """Mark a drift event as resolved."""
        for event in self._events:
            if event.event_id == event_id:
                event.status = "resolved"
                event.resolved_by = resolved_by
                event.resolved_at = datetime.now(timezone.utc).isoformat()
                break


# Standard drift policies
STANDARD_DRIFT_POLICIES = [
    DriftPolicy(
        name="schema_drift_production",
        drift_type=DriftType.SCHEMA,
        detection_enabled=True,
        detection_frequency="realtime",
        notify_owners=True,
        block_downstream=True,
        require_approval=True,
        apply_to_tags={"environment": "production"}
    ),
    DriftPolicy(
        name="schema_drift_development",
        drift_type=DriftType.SCHEMA,
        detection_enabled=True,
        detection_frequency="daily",
        notify_owners=False,
        block_downstream=False,
        require_approval=False,
        apply_to_tags={"environment": "development"}
    ),
    DriftPolicy(
        name="freshness_sla",
        drift_type=DriftType.FRESHNESS,
        detection_enabled=True,
        detection_frequency="hourly",
        warning_threshold=1.5,  # 1.5x expected frequency
        error_threshold=2.0,    # 2x expected frequency
        notify_owners=True,
        apply_to_tags={"data_quality": "gold"}
    ),
    DriftPolicy(
        name="quality_monitoring",
        drift_type=DriftType.QUALITY,
        detection_enabled=True,
        detection_frequency="daily",
        warning_threshold=0.05,  # 5% change
        error_threshold=0.10,    # 10% change
        notify_owners=True,
        apply_to_tags={"data_quality": "gold"}
    ),
]


def create_standard_drift_detector() -> DriftDetector:
    """Create detector with standard policies."""
    detector = DriftDetector()
    for policy in STANDARD_DRIFT_POLICIES:
        detector.register_policy(policy)
    return detector
