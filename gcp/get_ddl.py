from google.cloud import bigquery

client = bigquery.Client()

PROJECT_ID = "xxxxxxx"
DATASET_ID = "xxxxxxx"
TABLE_ID = "xxxxxxx"

dataset = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

get_ddl = client.get_table(dataset).schema()

print(get_ddl)
