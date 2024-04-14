import requests
import pandas as pd
from google.cloud import bigquery
import logging



OG_USERNAME = 'XXXXX'
OG_PASSWORD = 'XXXXX'
OG_ACCOUNT_CODE = 'XXXXX'


def load_to_bq(df, table_id, bigquery_table_schema=None):
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        # schema=bigquery_table_schema,
        write_disposition="WRITE_TRUNCATE"
    )
    # job_config.autodetect = True


    job = bq_client.load_table_from_dataframe(
        dataframe=df,
        destination=table_id,
        job_config=job_config
    )
    
    job.result()
    table = bq_client.get_table(table_id)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}')

# Define headers
headers = {
    'x_username': OG_USERNAME,
    'x_password': OG_PASSWORD,
    'x_account_code': OG_ACCOUNT_CODE
}

params = {"limit": 10000, "offset": 123000}


## query the 'contact_serach' that created
# url = 'https://api.ongage.net/214528/api/contact_search/2124873242/result?offset=0&limit=75&_=1712845990416'

url = f"https://api.ongage.net/214528/api/contact_search/2124873242/export"


max_results = 10000  # The maximum number of results per request
start_at = 0

contacts_data = []  # Move initialization outside the loop

data=['x']
while data:
  params = {"limit": max_results, "offset": start_at}
  response = requests.get(url, headers=headers, params=params)
  res = response.json()
  data = res['payload']
  print(start_at, len(data))

  contacts_data += data

  total_rows = res['metadata']['total']
  start_at += max_results
  if start_at >= total_rows:
        break  # All contacts have been fetched


print(f"------> Total contacts: {len(contacts_data)}")

df = pd.DataFrame(contacts_data)
table_id = 'XXXXX.mrr.ongage_contacts'
load_to_bq(df=df, table_id=table_id)
