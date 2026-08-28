# Datalake Module

Simple Azure Data Lake Storage client for MedalFlow.

Requires the optional cloud dependencies: `pip install 'medalflow[azure]'`. Importing this
module works without them -- the Azure SDKs and pandas are pulled in by the methods that use
them, and calling one without the extra installed raises a MedalFlow error naming the
install command.

## Usage

```python
from medalflow.datalake import get_processed_datalake_client, get_internal_datalake_client
import pandas as pd

client = get_processed_datalake_client()

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
internal = get_internal_datalake_client()
internal.upload_csv(df, 'raw/data.csv')
```

## Configuration

Configured via `MEDALFLOW_DATALAKE__PROCESSED__*` and `MEDALFLOW_DATALAKE__INTERNAL__*`
environment variables. See `medalflow.settings.datalake` for details.