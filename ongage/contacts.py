import time
import logging
import requests
import pandas as pd
import tempfile
from google.cloud import bigquery


OG_USERNAME = 'XXXXX'
OG_PASSWORD = 'XXXXX'
OG_ACCOUNT_CODE = 'XXXXX'


# Define headers
HEADERS = {
    'x_username': OG_USERNAME,
    'x_password': OG_PASSWORD,
    'x_account_code': OG_ACCOUNT_CODE
}

START_AT = 0
MAX_RESULTS = 50000

def generate_cm_report(headers=HEADERS):
    payload = {
      "title": "Active gmail contacts- testing IE",
      "selected_fields": [ "ocx_created_date", "email", "first_name", "last_name", "gender", "account_id" ],
      "filters": {
        "criteria": [
            {
              "type": "email",
              "field_name": "email",
              "operator": "notempty",
              "operand": ["email"],
              "case_sensitive": 0,
              "condition": "and"
            }
        ],
        "user_type":"active"
      }
      }
    
    url = 'https://api.ongage.net/api/contact_search'
    response = requests.post(url, headers=headers, json=payload)
    res = response.json()
    report_id = res['payload']['id']
    print(f"Processing report_id: {report_id}")

    return report_id
    
def check_cm_report_status(report_id=None, headers=HEADERS):
  report_status = 0
  url = f"https://api.ongage.net/214528/api/contact_search/{report_id}"

  while report_status <= 1:
    time.sleep(2)
    response = requests.get(url, headers=headers)
    res = response.json()
    report_status = res['payload']['status']

def fetch_cm_report(report_id, headers=HEADERS, start_at=START_AT, max_results=MAX_RESULTS):
  url = f"https://api.ongage.net/214528/api/contact_search/{report_id}/result"  ##?offset=0&limit=75&_=2208941999000"
  
  max_results = max_results
  start_at = start_at
  contacts_data = []

  while True:
    params = {"limit": max_results, "offset": start_at}
    print('Fetching...')
    response = requests.get(url, headers=headers, params=params)
    res = response.json()
    data = res['payload']
    total_rows = res['metadata']['total']
    print(total_rows, start_at, len(data))

    contacts_data += data    
    start_at += max_results
    if start_at >= total_rows:
          break  # All contacts have been fetched
    
  return contacts_data

def delete_cm_report(report_id=None, headers=HEADERS):
  url = f"https://api.ongage.net/214528/api/contact_search/{report_id}"
  response = requests.delete(url, headers=headers)
  return response


def create_df(data):
  df = pd.DataFrame(data)
  return df



def load_to_bq(df, table_id):
    bq_client = bigquery.Client(project='tangome-staging')
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )

    job = bq_client.load_table_from_dataframe(
        dataframe=df,
        destination=table_id,
        job_config=job_config
    )
    
    job.result()
    table = bq_client.get_table(table_id)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}')


if __name__ == '__main__':

  # exporting onGage contacts report
  report_id = generate_cm_report()
  check_cm_report_status(report_id=report_id)

  # report_id = 2131119914
  contacts_data = fetch_cm_report(report_id=report_id)
  
  df = create_df(contacts_data)
  # delete_cm_report(report_id=report_id)
  
  # # load df to bigQuery
  table_id = 'tangome-staging.ods.ongage_contacts'
  load_to_bq(df=df, table_id=table_id)
