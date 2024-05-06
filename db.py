import pyarrow as pa
import pyarrow.parquet as pq

def read_tb(table_name='indexer'):
    table_name += ".parquet"  # Append file extension
    try:
        pq_instance = pq.ParquetFile(table_name)
        return pq_instance.read().to_pandas()
    except Exception as e:
        print(f"Failed to read table {table_name}: {e}")
        return None  # Return None or an empty DataFrame as a fail-safe

    
def write_tb(df, table_name='indexer'):
    try:
        table_name = table_name + ".parquet"
        df.to_parquet(table_name)
        print(f"Table '{table_name}' successfully written.")
    except Exception as e:
        print(f"Error occurred while writing table '{table_name}': {e}")
