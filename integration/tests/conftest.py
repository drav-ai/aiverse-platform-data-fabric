"""
Pytest configuration for Data Fabric integration tests.
"""

import sys
from pathlib import Path

# Add parent directories to path for imports
base_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(base_path))
sys.path.insert(0, str(base_path.parent))
