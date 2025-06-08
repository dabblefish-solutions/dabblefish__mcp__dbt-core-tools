"""Schema version detection and information for DBT manifests.

This module provides utilities for detecting and handling different DBT
manifest schema versions with their specific capabilities and features.

Author: David B
Company: DABBLEFISH LLC
License: MIT
"""

import re
import sys
from typing import Any, Dict


class SchemaVersionInfo:
    """Schema version information and capabilities for DBT manifests.
    
    This class encapsulates the version-specific features and capabilities
    of different DBT manifest schema versions, enabling adaptive parsing
    and processing logic.
    
    Attributes:
        version: The detected schema version number
        has_parent_map: Whether this version supports parent_map
        has_child_map: Whether this version supports child_map
        node_structure: "modern" or "legacy" node structure
        metadata_location: "metadata" or "root" for metadata location
    """
    
    def __init__(self, version: int) -> None:
        """Initialize schema version information.
        
        Args:
            version: The DBT manifest schema version number
        """
        self.version = version
        self.has_parent_map = version >= 4
        self.has_child_map = version >= 4
        self.node_structure = "modern" if version >= 4 else "legacy"
        self.metadata_location = "metadata" if version >= 1 else "root"
    
    def __str__(self) -> str:
        """Return string representation of schema version."""
        return f"DBT Schema v{self.version}"
    
    def __repr__(self) -> str:
        """Return detailed representation of schema version."""
        return (
            f"SchemaVersionInfo(version={self.version}, "
            f"has_parent_map={self.has_parent_map}, "
            f"has_child_map={self.has_child_map}, "
            f"node_structure='{self.node_structure}', "
            f"metadata_location='{self.metadata_location}')"
        )


def detect_schema_version(manifest: Dict[str, Any]) -> SchemaVersionInfo:
    """Detect DBT manifest schema version from manifest data.
    
    This function analyzes the manifest structure to determine the schema
    version, enabling version-adaptive processing logic.
    
    Args:
        manifest: The parsed DBT manifest dictionary
        
    Returns:
        SchemaVersionInfo object with version details and capabilities
        
    Note:
        Defaults to v12 (latest) if version cannot be determined
    """
    version = 12  # Default to latest version
    
    metadata = manifest.get("metadata", {})
    if schema_version := metadata.get("dbt_schema_version"):
        if version_match := re.search(r"v(\d+)", schema_version):
            version = int(version_match.group(1))
    
    print(
        f"Detected DBT manifest schema version: v{version}",
        file=sys.stderr
    )
    return SchemaVersionInfo(version)