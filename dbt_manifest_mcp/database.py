"""Database operations for DBT manifest storage and querying.

This module provides the DbtManifestDB class for storing and querying
DBT manifest data in SQLite with version-adaptive parsing capabilities.

Author: David B
Company: DABBLEFISH LLC
License: MIT
"""

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .schema_version import SchemaVersionInfo, detect_schema_version


class DbtManifestDB:
    """Database helper class with version detection and adaptive parsing.
    
    This class manages SQLite database operations for storing and querying
    DBT manifest data, with automatic schema version detection and
    version-adaptive parsing logic.
    
    Attributes:
        db_path: Path to the SQLite database file
        db: SQLite database connection
    """
    
    def __init__(self, db_path: str) -> None:
        """Initialize database connection and create tables.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self._initialize_tables()
    
    def _initialize_tables(self) -> None:
        """Initialize database tables for manifest data storage."""
        cursor = self.db.cursor()
        
        # Metadata table for version info and manifest metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Nodes table for DBT models, tests, and other nodes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                unique_id TEXT PRIMARY KEY,
                name TEXT,
                resource_type TEXT,
                package_name TEXT,
                path TEXT,
                original_file_path TEXT,
                compiled_code TEXT,
                raw_code TEXT,
                database_name TEXT,
                schema_name TEXT,
                alias TEXT,
                full_data TEXT
            )
        """)
        
        # Sources table for DBT source definitions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                unique_id TEXT PRIMARY KEY,
                name TEXT,
                source_name TEXT,
                package_name TEXT,
                database_name TEXT,
                schema_name TEXT,
                full_data TEXT
            )
        """)
        
        # Macros table for DBT macro definitions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS macros (
                unique_id TEXT PRIMARY KEY,
                name TEXT,
                package_name TEXT,
                path TEXT,
                original_file_path TEXT,
                macro_sql TEXT,
                full_data TEXT
            )
        """)
        
        # Parent map table for upstream dependencies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS parent_map (
                child_id TEXT,
                parent_id TEXT,
                PRIMARY KEY (child_id, parent_id)
            )
        """)
        
        # Child map table for downstream dependencies
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS child_map (
                parent_id TEXT,
                child_id TEXT,
                PRIMARY KEY (parent_id, child_id)
            )
        """)
        
        self.db.commit()
    
    def _extract_lineage_maps(
        self,
        manifest: Dict[str, Any],
        version_info: SchemaVersionInfo
    ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """Extract parent/child maps from manifest based on version.
        
        Args:
            manifest: The parsed DBT manifest dictionary
            version_info: Schema version information and capabilities
            
        Returns:
            Tuple of (parent_map, child_map) dictionaries
        """
        parent_map: Dict[str, List[str]] = {}
        child_map: Dict[str, List[str]] = {}
        
        # Use native maps if available (v4+)
        if version_info.has_parent_map and "parent_map" in manifest:
            parent_map = manifest["parent_map"]
        
        if version_info.has_child_map and "child_map" in manifest:
            child_map = manifest["child_map"]
        
        # For older versions, build lineage from node dependencies
        if not version_info.has_parent_map or not version_info.has_child_map:
            print(
                "Building lineage maps from node dependencies (legacy version)",
                file=sys.stderr
            )
            
            for node_id, node in manifest.get("nodes", {}).items():
                depends_on = node.get("depends_on", {}).get("nodes", [])
                if depends_on:
                    # Build parent map
                    parent_map[node_id] = depends_on
                    
                    # Build child map
                    for parent_id in depends_on:
                        if parent_id not in child_map:
                            child_map[parent_id] = []
                        child_map[parent_id].append(node_id)
        
        return parent_map, child_map
    
    def _extract_node_properties(
        self,
        node: Dict[str, Any],
        version_info: SchemaVersionInfo
    ) -> Dict[str, Optional[str]]:
        """Safely extract node properties based on schema version.
        
        Args:
            node: Node data from manifest
            version_info: Schema version information
            
        Returns:
            Dictionary of extracted node properties
        """
        return {
            "compiled_code": (
                node.get("compiled_code") or node.get("compiled_sql")
            ),
            "raw_code": (
                node.get("raw_code") or 
                node.get("raw_sql") or 
                node.get("sql", "")
            ),
            "database": node.get("database") or node.get("database_name"),
            "schema": node.get("schema") or node.get("schema_name"),
            "alias": node.get("alias") or node.get("name"),
        }
    
    def refresh_manifest(self, manifest_path: Optional[str] = None) -> str:
        """Refresh DBT manifest data by parsing and storing in database.
        
        Args:
            manifest_path: Path to the DBT manifest.json file
            
        Returns:
            Success message with loading statistics
            
        Raises:
            ValueError: If manifest_path is not provided and no default set
            Exception: If manifest parsing or database operations fail
        """
        if not manifest_path:
            raise ValueError(
                "manifest_path must be provided or DBT_MANIFEST_PATH "
                "environment variable must be set"
            )
        
        try:
            # Read and parse manifest
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            # Detect schema version
            version_info = detect_schema_version(manifest)
            
            # Extract lineage maps
            parent_map, child_map = self._extract_lineage_maps(
                manifest, version_info
            )
            
            cursor = self.db.cursor()
            
            # Clear existing data
            cursor.execute("DELETE FROM metadata")
            cursor.execute("DELETE FROM nodes")
            cursor.execute("DELETE FROM sources")
            cursor.execute("DELETE FROM macros")
            cursor.execute("DELETE FROM parent_map")
            cursor.execute("DELETE FROM child_map")
            
            # Insert metadata with version info
            cursor.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?)",
                ("detected_schema_version", str(version_info.version))
            )
            cursor.execute(
                "INSERT INTO metadata (key, value) VALUES (?, ?)",
                ("schema_version_info", json.dumps({
                    "version": version_info.version,
                    "hasParentMap": version_info.has_parent_map,
                    "hasChildMap": version_info.has_child_map,
                    "nodeStructure": version_info.node_structure,
                    "metadataLocation": version_info.metadata_location,
                }))
            )
            
            # Store original metadata
            if metadata := manifest.get("metadata"):
                for key, value in metadata.items():
                    cursor.execute(
                        "INSERT INTO metadata (key, value) VALUES (?, ?)",
                        (f"original_{key}", json.dumps(value))
                    )
            
            # Insert nodes with version-aware property extraction
            if nodes := manifest.get("nodes"):
                for node_id, node in nodes.items():
                    props = self._extract_node_properties(node, version_info)
                    
                    cursor.execute("""
                        INSERT INTO nodes (
                            unique_id, name, resource_type, package_name, path,
                            original_file_path, compiled_code, raw_code,
                            database_name, schema_name, alias, full_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        node.get("unique_id", node_id),
                        node.get("name"),
                        node.get("resource_type"),
                        node.get("package_name"),
                        node.get("path") or node.get("original_file_path"),
                        node.get("original_file_path"),
                        props["compiled_code"],
                        props["raw_code"],
                        props["database"],
                        props["schema"],
                        props["alias"],
                        json.dumps(node)
                    ))
            
            # Insert sources
            if sources := manifest.get("sources"):
                for source_id, source in sources.items():
                    cursor.execute("""
                        INSERT INTO sources (
                            unique_id, name, source_name, package_name,
                            database_name, schema_name, full_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        source.get("unique_id", source_id),
                        source.get("name"),
                        source.get("source_name"),
                        source.get("package_name"),
                        source.get("database"),
                        source.get("schema"),
                        json.dumps(source)
                    ))
            
            # Insert macros
            if macros := manifest.get("macros"):
                for macro_id, macro in macros.items():
                    cursor.execute("""
                        INSERT INTO macros (
                            unique_id, name, package_name, path,
                            original_file_path, macro_sql, full_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        macro.get("unique_id", macro_id),
                        macro.get("name"),
                        macro.get("package_name"),
                        macro.get("path"),
                        macro.get("original_file_path"),
                        macro.get("macro_sql") or macro.get("sql", ""),
                        json.dumps(macro)
                    ))
            
            # Insert parent map
            for child_id, parent_ids in parent_map.items():
                for parent_id in parent_ids:
                    cursor.execute(
                        "INSERT INTO parent_map (child_id, parent_id) "
                        "VALUES (?, ?)",
                        (child_id, parent_id)
                    )
            
            # Insert child map
            for parent_id, child_ids in child_map.items():
                for child_id in child_ids:
                    cursor.execute(
                        "INSERT INTO child_map (parent_id, child_id) "
                        "VALUES (?, ?)",
                        (parent_id, child_id)
                    )
            
            self.db.commit()
            
            # Generate statistics
            node_count = len(manifest.get("nodes", {}))
            source_count = len(manifest.get("sources", {}))
            macro_count = len(manifest.get("macros", {}))
            parent_map_count = len(parent_map)
            child_map_count = len(child_map)
            
            return (
                f"Successfully refreshed manifest data from {manifest_path}\n"
                f"Schema Version: v{version_info.version}\n"
                f"Loaded: {node_count} nodes, {source_count} sources, "
                f"{macro_count} macros\n"
                f"Lineage: {parent_map_count} parent relationships, "
                f"{child_map_count} child relationships"
            )
            
        except Exception as e:
            raise Exception(f"Failed to refresh manifest: {str(e)}") from e
    
    def get_upstream_lineage(self, model_id: str) -> List[str]:
        """Get upstream lineage for a DBT model.
        
        Args:
            model_id: Unique ID of the DBT model
            
        Returns:
            List of upstream model IDs
        """
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT parent_id FROM parent_map WHERE child_id = ?",
            (model_id,)
        )
        return [row["parent_id"] for row in cursor.fetchall()]
    
    def get_downstream_lineage(self, model_id: str) -> List[str]:
        """Get downstream lineage for a DBT model.
        
        Args:
            model_id: Unique ID of the DBT model
            
        Returns:
            List of downstream model IDs
        """
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT child_id FROM child_map WHERE parent_id = ?",
            (model_id,)
        )
        return [row["child_id"] for row in cursor.fetchall()]
    
    def get_model_info(self, model_id: str) -> Dict[str, Any]:
        """Get detailed information about a DBT model.
        
        Args:
            model_id: Unique ID of the DBT model
            
        Returns:
            Dictionary with detailed model information
            
        Raises:
            ValueError: If model is not found
        """
        cursor = self.db.cursor()
        
        # Get model details
        cursor.execute("SELECT * FROM nodes WHERE unique_id = ?", (model_id,))
        model_row = cursor.fetchone()
        
        if not model_row:
            raise ValueError(f"Model {model_id} not found")
        
        # Get parent count
        cursor.execute(
            "SELECT COUNT(*) as parent_count FROM parent_map "
            "WHERE child_id = ?",
            (model_id,)
        )
        parent_count = cursor.fetchone()["parent_count"]
        
        # Get child count
        cursor.execute(
            "SELECT COUNT(*) as child_count FROM child_map "
            "WHERE parent_id = ?",
            (model_id,)
        )
        child_count = cursor.fetchone()["child_count"]
        
        return {
            "unique_id": model_row["unique_id"],
            "name": model_row["name"],
            "resource_type": model_row["resource_type"],
            "package_name": model_row["package_name"],
            "path": model_row["path"],
            "compiled_code": model_row["compiled_code"],
            "parent_count": parent_count,
            "child_count": child_count,
            "database": model_row["database_name"],
            "schema": model_row["schema_name"],
            "alias": model_row["alias"],
        }
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get information about the loaded DBT manifest schema version.
        
        Returns:
            Dictionary with version info, features, and statistics
        """
        cursor = self.db.cursor()
        
        # Get version info
        cursor.execute("""
            SELECT key, value FROM metadata 
            WHERE key IN (
                'detected_schema_version', 
                'schema_version_info', 
                'original_dbt_schema_version'
            )
        """)
        
        result = {
            "detected_version": None,
            "version_info": None,
            "original_schema_version": None,
            "supported_features": [],
            "database_stats": {}
        }
        
        for row in cursor.fetchall():
            if row["key"] == "detected_schema_version":
                result["detected_version"] = int(row["value"])
            elif row["key"] == "schema_version_info":
                result["version_info"] = json.loads(row["value"])
            elif row["key"] == "original_dbt_schema_version":
                result["original_schema_version"] = json.loads(row["value"])
        
        # Get database statistics
        stats_queries = [
            ("nodes", "SELECT COUNT(*) as count FROM nodes"),
            ("sources", "SELECT COUNT(*) as count FROM sources"),
            ("macros", "SELECT COUNT(*) as count FROM macros"),
            ("parent_relationships", "SELECT COUNT(*) as count FROM parent_map"),
            ("child_relationships", "SELECT COUNT(*) as count FROM child_map"),
        ]
        
        for stat_name, query in stats_queries:
            cursor.execute(query)
            result["database_stats"][stat_name] = cursor.fetchone()["count"]
        
        # Determine supported features
        if result["version_info"]:
            result["supported_features"] = [
                "node_parsing",
                "source_parsing",
                "macro_parsing"
            ]
            
            if result["version_info"]["hasParentMap"]:
                result["supported_features"].append("parent_lineage")
            
            if result["version_info"]["hasChildMap"]:
                result["supported_features"].append("child_lineage")
            
            if result["version_info"]["nodeStructure"] == "modern":
                result["supported_features"].append("modern_node_structure")
            else:
                result["supported_features"].append("legacy_node_structure")
        
        return result