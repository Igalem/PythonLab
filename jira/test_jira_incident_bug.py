import requests
import pandas as pd
import numpy as np
import base64
import logging
from google.cloud import bigquery
from config.jira_fields_list_dict import *


jira_cred = {"api_token": "XXXXX", "username": "XXXXX", "jira_url": "XXXXX"}
username = jira_cred['username']
api_token = jira_cred['api_token']
jira_url = jira_cred['jira_url']

URL = f"{jira_url}/rest/api/2/search"

credentials = f"{username}:{api_token}"
base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
# JIRA_QUERY = "updated >= -1d" ## and issuetype in ('Production incident','ANR','Crash','Defect','Memory leak','Bug')"
JIRA_QUERY = "issue = 'OPS-24989'"

table_full_name = 'XXXXX.mrr.jira_incident_bug'

headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {base64_credentials}",
    }


def stringHandler(string=None):
    string = '' if string is None else string.replace("'", "")
    return string

def get_field_values(row, field_name, field_key=None):
    try:
        if isinstance(row[field_name], list):
            if isinstance(row[field_name][0], dict) and field_key is None:
                field_values = str(row[field_name])
            elif isinstance(row[field_name][0], dict):
                field_values = str(row[field_name][0][field_key])
            else:
                field_values = row[field_name]
        elif field_key is not None:
            field_values = str(row[field_name][field_key])
        else:
            field_values = str(row[field_name])
    except:
        field_values = ''
    return field_values

def get_str_from_list(field_values, field_key=None):
    try:
        if isinstance(field_values, str) and field_values[0] == '{':
            return field_values
        elif isinstance(field_values, list):
            field_str_list = [str(value[field_key]) for value in field_values]
            return stringHandler(string=','.join(field_str_list))
        else:
            return stringHandler(string=field_values)
    except:
        field_str_list = [str(value) for value in field_values]
        return stringHandler(string=','.join(field_str_list))

def get_issue_keys(data):
    issue_keys = []
    for item in data:
        if 'outwardIssue' in item:
            issue_keys.append(item['outwardIssue']['key'])
        elif 'inwardIssue' in item:
            issue_keys.append(item['inwardIssue']['key'])
    return issue_keys

def fn_load_to_bigquery(df, table_full_name, bigquery_table_schema=None):
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        # schema=bigquery_table_schema,
        write_disposition="WRITE_TRUNCATE"
    )
    # job_config.autodetect = True


    job = bq_client.load_table_from_dataframe(
        dataframe=df,
        destination=table_full_name,
        job_config=job_config
    )
    
    job.result()
    table = bq_client.get_table(table_full_name)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_full_name}')

max_results = 100  # The maximum number of results per request
start_at = 0
all_issues_data = []

issues_data = []  # Move initialization outside the loop
jira_fields = [field for field in jira_fields_list]

while True:
    payload = {
        "jql": JIRA_QUERY,
        "fields": jira_fields,
        "startAt": start_at,
        "maxResults": max_results
    }

    response = requests.post(URL, headers=headers, json=payload)
    data = response.json()
    # print(data)

    for issue in data['issues']:
        row_data = {}
        issue_key = issue['key']
        issue_url = f"{jira_url}/browse/{issue_key}"

        row_data['issue'] = issue_key
        row_data['url'] = issue_url

        fields = issue['fields']

        for field_name in fields:
            field_key = jira_fields_list[field_name]

            if field_name == 'issuelinks':
                field_values = get_issue_keys(fields[field_name])
            else:
                field_values = get_field_values(row=fields, field_name=field_name, field_key=field_key)

            #--------------------------------------------
            # print(field_name, type(field_values))
            # print(field_values, '\n')
            #--------------------------------------------            

            field_value = get_str_from_list(field_values=field_values, field_key=field_key)

            mapped_field_name = jira_mapping_fields[field_name]
            row_data[mapped_field_name] = field_value
        
        issues_data.append(row_data)
        # print(row_data)

    start_at += max_results
    if start_at >= data['total']:
        break  # All issues have been retrieved



df = pd.DataFrame(issues_data).fillna(value='')  ##replace({None: np.nan})  ###
df = df.astype(str)


fn_load_to_bigquery(df, table_full_name,)