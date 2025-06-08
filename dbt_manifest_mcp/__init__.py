"""DBT Manifest MCP Server.

A FastMCP server for analyzing DBT manifests with automatic schema version
detection and lineage tracking.

Author: David B
Company: DABBLEFISH LLC
License: MIT
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "David B"
__company__ = "DABBLEFISH LLC"
__license__ = "MIT"

from .server import main
from .database import DbtManifestDB
from .schema_version import SchemaVersionInfo

__all__ = ["main", "DbtManifestDB", "SchemaVersionInfo"]