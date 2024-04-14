from jira import JIRA
import sys 
import os
sys.path.append(os.path.abspath("/Users/iemona/vscode/PythonLab"))

from datetime import datetime
from config.jira_fields_list_dict import *
import json
import pandas as pd


JIRA_SERVER = 'XXXXX'
USERNAME = 'XXXXX'
PASSWORD = 'XXXXX'


def stringHandler(string=None):
    string = '' if string is None else string.replace("'", "")
    return string

def get_field_values(row, field_name, field_key=None):
    try:
        if isinstance(row[field_name], list):
            if isinstance(row[field_name][0], dict):
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
        if isinstance(field_values, list):
            field_str_list = [str(value[field_key]) for value in field_values]
            return stringHandler(string=','.join(field_str_list))
        else:
            return stringHandler(string=field_values)
    except:
        field_str_list = [str(value) for value in field_values]
        return stringHandler(string=','.join(field_str_list))    
    

fields = [field for field in jira_fields_list]

# Create a Jira client instance
jira = JIRA(server=JIRA_SERVER, basic_auth=(USERNAME, PASSWORD))

# jql_query = "created >= '2024-03-01'"
# jql_query = "issuetype in ('Production incident','ANR','Crash','Defect','Memory leak','Bug') AND created >= -2d"
jql_query = "key='DEP-1126'"

issues = jira.search_issues(jql_query, maxResults=None)
data = []
for issue in issues:
    row_data = []
    row_data.append(issue.key)
    issue_raw = json.dumps(jira.issue(issue.key).raw)
    issue = json.loads(issue_raw)['fields']
    for field_name in fields:
        field_value = get_field_values(row=issue, field_name=field_name, field_key=jira_fields_list[field_name])
        row_data.append(field_value)

    data.append(row_data)

print(issue)    

# df = pd.DataFrame(data)
# csv_filename = '/Users/iemona/temp/jira_issues.csv'

# sub_headers = [jira_mapping_fields[col] for col in jira_mapping_fields]
# headers = ['issue'] + sub_headers

# df.to_csv(csv_filename, header=headers, index=False)