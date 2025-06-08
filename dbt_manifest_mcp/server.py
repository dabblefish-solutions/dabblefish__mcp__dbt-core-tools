"""FastMCP server for DBT manifest analysis.

This module provides the main FastMCP server implementation with tools
for analyzing DBT manifests, including lineage tracking and model information.

Author: David B
Company: DABBLEFISH LLC
License: MIT
"""

import os
from typing import Any, Dict

from fastmcp import FastMCP

from .database import DbtManifestDB


# Configuration from environment variables
MANIFEST_PATH = os.getenv("DBT_MANIFEST_PATH")
DB_PATH = os.getenv("DBT_DB_PATH", "./dbt_manifest.db")

# Initialize FastMCP server
mcp = FastMCP("DBT Manifest Server")

# Initialize database
db = DbtManifestDB(DB_PATH)


@mcp.tool()
def refresh_manifest(manifest_path: str = None) -> str:
    """Refresh DBT manifest data by parsing and storing in SQLite database.
    
    This tool parses a DBT manifest.json file and stores the data in a SQLite
    database for efficient querying. It automatically detects the schema version
    and adapts the parsing logic accordingly.
    
    Args:
        manifest_path: Path to the DBT manifest.json file (optional, uses
                      configured path if not provided)
    
    Returns:
        Success message with statistics about loaded data
        
    Raises:
        ValueError: If no manifest path is provided or configured
        Exception: If manifest parsing or database operations fail
    """
    path = manifest_path or MANIFEST_PATH
    return db.refresh_manifest(path)


@mcp.tool()
def get_upstream_lineage(model_id: str) -> Dict[str, Any]:
    """Get upstream lineage for a DBT model.
    
    This tool retrieves all upstream dependencies (parent models) for a
    specified DBT model, providing insight into the data flow and dependencies.
    
    Args:
        model_id: Unique ID of the DBT model (e.g., 'model.my_project.my_model')
    
    Returns:
        Dictionary containing:
        - model_id: The input model ID
        - upstream_models: List of upstream model IDs
        - count: Number of upstream models
    """
    upstream = db.get_upstream_lineage(model_id)
    return {
        "model_id": model_id,
        "upstream_models": upstream,
        "count": len(upstream)
    }


@mcp.tool()
def get_downstream_lineage(model_id: str) -> Dict[str, Any]:
    """Get downstream lineage for a DBT model.
    
    This tool retrieves all downstream dependencies (child models) for a
    specified DBT model, showing what models depend on this one.
    
    Args:
        model_id: Unique ID of the DBT model (e.g., 'model.my_project.my_model')
    
    Returns:
        Dictionary containing:
        - model_id: The input model ID
        - downstream_models: List of downstream model IDs
        - count: Number of downstream models
    """
    downstream = db.get_downstream_lineage(model_id)
    return {
        "model_id": model_id,
        "downstream_models": downstream,
        "count": len(downstream)
    }


@mcp.tool()
def get_model_info(model_id: str) -> Dict[str, Any]:
    """Get detailed information about a DBT model.
    
    This tool provides comprehensive information about a specific DBT model,
    including metadata, compiled code, and dependency counts.
    
    Args:
        model_id: Unique ID of the DBT model (e.g., 'model.my_project.my_model')
    
    Returns:
        Dictionary with detailed model information including:
        - unique_id: Model's unique identifier
        - name: Model name
        - resource_type: Type of DBT resource (model, test, etc.)
        - package_name: DBT package name
        - path: File path within the project
        - compiled_code: Compiled SQL code
        - parent_count: Number of upstream dependencies
        - child_count: Number of downstream dependencies
        - database: Target database name
        - schema: Target schema name
        - alias: Model alias
        
    Raises:
        ValueError: If the specified model is not found
    """
    return db.get_model_info(model_id)


@mcp.tool()
def get_schema_info() -> Dict[str, Any]:
    """Get information about the loaded DBT manifest schema version.
    
    This tool provides details about the currently loaded DBT manifest,
    including schema version, supported features, and database statistics.
    
    Returns:
        Dictionary containing:
        - detected_version: Detected schema version number
        - version_info: Detailed version capabilities
        - original_schema_version: Original schema version from manifest
        - supported_features: List of supported features
        - database_stats: Statistics about loaded data (counts of nodes,
                         sources, macros, and relationships)
    """
    return db.get_schema_info()


def main() -> None:
    """Main entry point for the FastMCP server.
    
    This function starts the FastMCP server and handles incoming requests
    for DBT manifest analysis tools.
    """
    mcp.run()


if __name__ == "__main__":
    main()