# Datalake Module

Simple Azure Data Lake Storage client for MedalFlow.

## Usage

```python
from core.datalake import DatalakeFactory, get_processed_datalake_client, get_internal_datalake_client
import pandas as pd

# Using factory
factory = DatalakeFactory()
client = factory.get_processed_client()

# Or using convenience functions
processed_client = get_processed_datalake_client()
internal_client = get_internal_datalake_client()

# Upload DataFrame
df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
client.upload(df, 'data/myfile.parquet')

# Read DataFrame
df = client.read('data/myfile.parquet')

# Check if file exists
if client.exists('data/myfile.parquet'):
    client.delete('data/myfile.parquet')

# List files
files = client.list_files('data/', recursive=True)

# Use Internal lake
internal = factory.get_internal_client()
internal.upload_csv(df, 'raw/data.csv')
```

## Configuration

Configured via environment variables. See `core.settings.datalake` for details.