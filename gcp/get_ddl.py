from google.cloud import bigquery

client = bigquery.Client()

PROJECT_ID = "xxxxx"
DATASET_ID = "xxxxx"
TABLE_ID = "xxxxx"

dataset = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

get_ddl = client.get_table(dataset).schema()

print(get_ddl)
