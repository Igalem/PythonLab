import pandas as pd
import gcsfs

# Specify the URI
uri = 'gs://xxxxxxx/*.parquet'

# Create a GCS filesystem
fs = gcsfs.GCSFileSystem()

# List the files in the URI
files = fs.glob(uri)

# Read the Parquet files into a DataFrame
df = pd.concat(
    pd.read_parquet(fs.open(file), engine='pyarrow')
    for file in files
)

# Print the DataFrame
print(df)