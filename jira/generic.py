import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud.bigquery import enums


def generate_bq_schema(field_list):
    """
    Generate BigQuery schema from dictionary field list.

    Args:
        field_list (dict): Dictionary containing field names as keys and corresponding data types as values.

    Returns:
        list: BigQuery schema represented as a list of bigquery.SchemaField objects.
    """
    from google.cloud import bigquery

    schema = []
    for field_name, data_type in field_list.items():
        if data_type.lower() == "string":
            bq_data_type = "STRING"
        elif data_type.lower() == "int":
            bq_data_type = "INTEGER"
        elif data_type.lower() == "float":
            bq_data_type = "FLOAT"
        elif data_type.lower() == "bool":
            bq_data_type = "BOOLEAN"
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

        schema.append(bigquery.SchemaField(field_name, bq_data_type))

    return schema

def generate_bq_schema_with_enums(field_list):
    """
    Generate BigQuery schema from dictionary field list using enums from bigquery.enums.SqlTypeNames.

    Args:
        field_list (dict): Dictionary containing field names as keys and corresponding SQL type enums as values.

    Returns:
        list: BigQuery schema represented as a list of bigquery.SchemaField objects.
    """
    schema = []
    for field_name, sql_type_enum in field_list.items():
        bq_data_type = getattr(enums.SqlTypeNames, sql_type_enum.upper(), None)
        if bq_data_type is None:
            raise ValueError(f"Unsupported SQL type enum: {sql_type_enum}")
        schema.append(bigquery.SchemaField(field_name, bq_data_type))

    return schema


field_list = {
    "name": "STRING",
    "company": "STRING",
    "id": "INT64",
    "mm": "BOOLEAN",
    "perc": "FLOAT",
    "created": "DATE",
    "current_timestamp": "TIMESTAMP"
}

data = [{"name": "igal", "company": "", "id": 333, "mm": True, "perc": 623.234, "created": '2024-04-04', "current_timestamp": "2024-04-04 09:42:38.766"}]

df = pd.DataFrame(data)
# df['created'] = pd.to_datetime(df['created'])
# df['current_timestamp'] = pd.to_datetime(df['current_timestamp'])


df1 = df.fillna(value=np.nan)

# schema = generate_bq_schema(field_list)
schema = generate_bq_schema_with_enums(field_list)

print(schema)

table_id = 'XXXXX.mrr.generic'

client = bigquery.Client()
# job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(df1, table_id, job_config=job_config)
job.result()  # Wait for the job to complete.
table = client.get_table(table_id)  # Make an API request.

