# Example DBT Manifests

This directory contains example DBT manifest files for testing and demonstration purposes.

**Author**: David B  
**Company**: DABBLEFISH LLC

## Files

### example_manifest_v2.json
- **Schema Version**: v2 (Legacy)
- **Features**: Basic node structure, no native parent/child maps
- **Use Case**: Testing backward compatibility with older DBT versions
- **Lineage**: Built from node dependencies

### example_manifest_v4.json
- **Schema Version**: v4 (Modern)
- **Features**: Modern node structure, native parent_map and child_map
- **Use Case**: Testing current DBT functionality
- **Lineage**: Uses native lineage maps

## Usage

These files are used by the test suite to verify that the DBT Manifest MCP Server correctly handles different schema versions and can adapt its parsing logic accordingly.

```python
# Example usage in tests
from dbt_manifest_mcp.database import DbtManifestDB

db = DbtManifestDB("test.db")
db.refresh_manifest("docs/examples/example_manifest_v4.json")
```

## Schema Version Support

The server automatically detects the schema version and adapts its behavior:

| Version | Parent Map | Child Map | Node Structure | Metadata Location |
|---------|------------|-----------|----------------|-------------------|
| v2      | ❌ (built) | ❌ (built) | Legacy         | Metadata          |
| v4      | ✅ Native  | ✅ Native  | Modern         | Metadata          |

## Testing

These examples are used in the pytest test suite:

```bash
pytest tests/ -v
```

The tests verify:
- Schema version detection
- Version-adaptive parsing
- Lineage map construction
- Database operations
- Error handling