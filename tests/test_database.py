"""Tests for DBT Manifest MCP Server database operations.

Author: David B
Company: DABBLEFISH LLC
License: MIT
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from dbt_manifest_mcp.database import DbtManifestDB
from dbt_manifest_mcp.schema_version import SchemaVersionInfo, detect_schema_version


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    temp_db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    temp_db_file.close()
    db = DbtManifestDB(temp_db_file.name)
    yield db
    db.db.close()
    os.unlink(temp_db_file.name)


@pytest.fixture
def manifest_paths():
    """Get paths to test manifest files."""
    test_dir = Path(__file__).parent.parent
    return {
        'v2': test_dir / "docs" / "examples" / "example_manifest_v2.json",
        'v4': test_dir / "docs" / "examples" / "example_manifest_v4.json"
    }


class TestDbtManifestDB:
    """Test cases for DbtManifestDB class."""
    
    def test_database_initialization(self, temp_db):
        """Test that database tables are created correctly."""
        cursor = temp_db.db.cursor()
        
        # Check that all required tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        tables = {row[0] for row in cursor.fetchall()}
        
        expected_tables = {
            'metadata', 'nodes', 'sources', 'macros', 
            'parent_map', 'child_map'
        }
        assert tables == expected_tables
    
    def test_refresh_manifest_v2(self, temp_db, manifest_paths):
        """Test refreshing manifest with v2 schema."""
        if not manifest_paths['v2'].exists():
            pytest.skip("V2 manifest file not found")
        
        result = temp_db.refresh_manifest(str(manifest_paths['v2']))
        
        # Check that result contains expected information
        assert "Successfully refreshed" in result
        assert "Schema Version: v2" in result
        
        # Verify data was loaded
        cursor = temp_db.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM nodes")
        node_count = cursor.fetchone()[0]
        assert node_count > 0
    
    def test_refresh_manifest_v4(self, temp_db, manifest_paths):
        """Test refreshing manifest with v4 schema."""
        if not manifest_paths['v4'].exists():
            pytest.skip("V4 manifest file not found")
        
        result = temp_db.refresh_manifest(str(manifest_paths['v4']))
        
        # Check that result contains expected information
        assert "Successfully refreshed" in result
        assert "Schema Version: v4" in result
        
        # Verify data was loaded
        cursor = temp_db.db.cursor()
        cursor.execute("SELECT COUNT(*) FROM nodes")
        node_count = cursor.fetchone()[0]
        assert node_count > 0
    
    def test_get_upstream_lineage(self, temp_db, manifest_paths):
        """Test getting upstream lineage for a model."""
        if not manifest_paths['v4'].exists():
            pytest.skip("V4 manifest file not found")
        
        # Load test data
        temp_db.refresh_manifest(str(manifest_paths['v4']))
        
        # Get a model ID from the database
        cursor = temp_db.db.cursor()
        cursor.execute("SELECT unique_id FROM nodes LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            model_id = row[0]
            upstream = temp_db.get_upstream_lineage(model_id)
            assert isinstance(upstream, list)
    
    def test_get_downstream_lineage(self, temp_db, manifest_paths):
        """Test getting downstream lineage for a model."""
        if not manifest_paths['v4'].exists():
            pytest.skip("V4 manifest file not found")
        
        # Load test data
        temp_db.refresh_manifest(str(manifest_paths['v4']))
        
        # Get a model ID from the database
        cursor = temp_db.db.cursor()
        cursor.execute("SELECT unique_id FROM nodes LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            model_id = row[0]
            downstream = temp_db.get_downstream_lineage(model_id)
            assert isinstance(downstream, list)
    
    def test_get_model_info(self, temp_db, manifest_paths):
        """Test getting detailed model information."""
        if not manifest_paths['v4'].exists():
            pytest.skip("V4 manifest file not found")
        
        # Load test data
        temp_db.refresh_manifest(str(manifest_paths['v4']))
        
        # Get a model ID from the database
        cursor = temp_db.db.cursor()
        cursor.execute("SELECT unique_id FROM nodes LIMIT 1")
        row = cursor.fetchone()
        
        if row:
            model_id = row[0]
            model_info = temp_db.get_model_info(model_id)
            
            # Check required fields
            required_fields = [
                'unique_id', 'name', 'resource_type', 'package_name',
                'parent_count', 'child_count'
            ]
            for field in required_fields:
                assert field in model_info
    
    def test_get_model_info_not_found(self, temp_db):
        """Test getting model info for non-existent model."""
        with pytest.raises(ValueError):
            temp_db.get_model_info("non.existent.model")
    
    def test_get_schema_info(self, temp_db, manifest_paths):
        """Test getting schema information."""
        if not manifest_paths['v4'].exists():
            pytest.skip("V4 manifest file not found")
        
        # Load test data
        temp_db.refresh_manifest(str(manifest_paths['v4']))
        
        schema_info = temp_db.get_schema_info()
        
        # Check required fields
        required_fields = [
            'detected_version', 'version_info', 'supported_features',
            'database_stats'
        ]
        for field in required_fields:
            assert field in schema_info
        
        # Check that version was detected
        assert schema_info['detected_version'] is not None
        assert isinstance(schema_info['supported_features'], list)
        assert isinstance(schema_info['database_stats'], dict)
    
    def test_refresh_manifest_no_path(self, temp_db):
        """Test refresh_manifest with no path provided."""
        with pytest.raises(ValueError):
            temp_db.refresh_manifest(None)


class TestSchemaVersionInfo:
    """Test cases for SchemaVersionInfo class."""
    
    def test_version_2_capabilities(self):
        """Test capabilities for schema version 2."""
        version_info = SchemaVersionInfo(2)
        
        assert version_info.version == 2
        assert not version_info.has_parent_map
        assert not version_info.has_child_map
        assert version_info.node_structure == "legacy"
        assert version_info.metadata_location == "metadata"
    
    def test_version_4_capabilities(self):
        """Test capabilities for schema version 4."""
        version_info = SchemaVersionInfo(4)
        
        assert version_info.version == 4
        assert version_info.has_parent_map
        assert version_info.has_child_map
        assert version_info.node_structure == "modern"
        assert version_info.metadata_location == "metadata"
    
    def test_version_0_capabilities(self):
        """Test capabilities for schema version 0."""
        version_info = SchemaVersionInfo(0)
        
        assert version_info.version == 0
        assert not version_info.has_parent_map
        assert not version_info.has_child_map
        assert version_info.node_structure == "legacy"
        assert version_info.metadata_location == "root"
    
    def test_string_representation(self):
        """Test string representations of SchemaVersionInfo."""
        version_info = SchemaVersionInfo(4)
        
        assert str(version_info) == "DBT Schema v4"
        assert "SchemaVersionInfo" in repr(version_info)
        assert "version=4" in repr(version_info)


class TestSchemaVersionDetection:
    """Test cases for schema version detection."""
    
    def test_detect_version_from_metadata(self):
        """Test detecting version from manifest metadata."""
        manifest = {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v4.json"
            }
        }
        
        version_info = detect_schema_version(manifest)
        assert version_info.version == 4
    
    def test_detect_version_no_metadata(self):
        """Test detecting version when no metadata is present."""
        manifest = {}
        
        version_info = detect_schema_version(manifest)
        assert version_info.version == 12  # Default to latest
    
    def test_detect_version_invalid_format(self):
        """Test detecting version with invalid schema format."""
        manifest = {
            "metadata": {
                "dbt_schema_version": "invalid_format"
            }
        }
        
        version_info = detect_schema_version(manifest)
        assert version_info.version == 12  # Default to latest


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )