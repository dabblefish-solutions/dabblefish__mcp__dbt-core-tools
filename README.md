# DBT Manifest MCP Server

A FastMCP server for analyzing DBT manifests with automatic schema version detection and lineage tracking.

**Author**: David B
**Company**: DABBLEFISH LLC
**License**: MIT

## Features

- **Automatic Schema Version Detection**: Supports DBT manifest schema versions v0-v12
- **Version-Adaptive Parsing**: Backward compatibility with legacy manifest formats
- **SQLite Database Storage**: Efficient querying and data persistence
- **Lineage Analysis**: Upstream and downstream dependency tracking
- **Model Information**: Detailed model metadata and compiled code access
- **PEP-8 Compliant**: Professional Python package structure

## Installation

### From PyPI (when published)
```bash
pip install dbt-manifest-mcp
```

### From Source
```bash
git clone https://github.com/dabblefish/dbt-manifest-mcp.git
cd dbt-manifest-mcp
pip install -e .
```

### Development Installation
```bash
git clone https://github.com/dabblefish/dbt-manifest-mcp.git
cd dbt-manifest-mcp
pip install -e ".[dev]"
```

## Usage

### Running the Server

```bash
# Using the installed command
dbt-manifest-mcp

# Or using Python module
python -m dbt_manifest_mcp.server
```

### Environment Variables

- `DBT_MANIFEST_PATH`: Path to the DBT manifest.json file (required)
- `DBT_DB_PATH`: Path to SQLite database file (optional, defaults to ./dbt_manifest.db)

### Example
```bash
export DBT_MANIFEST_PATH="/path/to/your/manifest.json"
export DBT_DB_PATH="./dbt_manifest.db"
dbt-manifest-mcp
```

## Available Tools

### 1. refresh_manifest

Refresh DBT manifest data by parsing and storing in SQLite database.

**Parameters:**
- `manifest_path` (optional): Path to the DBT manifest.json file

**Returns:** Success message with statistics

### 2. get_upstream_lineage

Get upstream lineage for a DBT model.

**Parameters:**
- `model_id`: Unique ID of the DBT model (e.g., 'model.my_project.my_model')

**Returns:** Dictionary with model_id, upstream_models list, and count

### 3. get_downstream_lineage

Get downstream lineage for a DBT model.

**Parameters:**
- `model_id`: Unique ID of the DBT model (e.g., 'model.my_project.my_model')

**Returns:** Dictionary with model_id, downstream_models list, and count

### 4. get_model_info

Get detailed information about a DBT model including parent/child counts and compiled code.

**Parameters:**
- `model_id`: Unique ID of the DBT model (e.g., 'model.my_project.my_model')

**Returns:** Dictionary with detailed model information

### 5. get_schema_info

Get information about the loaded DBT manifest schema version, supported features, and database statistics.

**Returns:** Dictionary with version info, features, and statistics

## Schema Version Support

The server automatically detects and adapts to different DBT manifest schema versions:

- **v0-v3**: Legacy format with basic node structure
- **v4+**: Modern format with parent_map and child_map
- **v12**: Latest format with enhanced metadata

### Version-Specific Features

| Version | Parent Map | Child Map | Node Structure | Metadata Location |
|---------|------------|-----------|----------------|-------------------|
| v0-v3   | ❌ (built from dependencies) | ❌ (built from dependencies) | Legacy | Root |
| v4-v11  | ✅ | ✅ | Modern | Metadata |
| v12     | ✅ | ✅ | Modern | Metadata |

## Database Schema

The server creates the following SQLite tables:

- `metadata`: Schema version and manifest metadata
- `nodes`: DBT models, tests, and other nodes
- `sources`: DBT source definitions
- `macros`: DBT macro definitions
- `parent_map`: Parent-child relationships
- `child_map`: Child-parent relationships

## Example Usage

```python
# After starting the server, you can use the tools via MCP client

# Refresh manifest data
refresh_manifest("/path/to/manifest.json")

# Get upstream dependencies
upstream = get_upstream_lineage("model.my_project.customer_orders")

# Get downstream dependencies
downstream = get_downstream_lineage("model.my_project.raw_customers")

# Get detailed model information
model_info = get_model_info("model.my_project.customer_summary")

# Get schema version information
schema_info = get_schema_info()
```

## Error Handling

The server includes comprehensive error handling for:

- Missing or invalid manifest files
- Unsupported schema versions
- Database connection issues
- Invalid model IDs

## License

MIT License - see LICENSE file for details.