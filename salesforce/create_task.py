import csv
from simple_salesforce import Salesforce
from datetime import datetime, timedelta

# Salesforce Credentials:
USERNAME = "xxxxx"
PASSWORD = "xxxxx"
SECURITY_TOKEN = "xxxxxxxxxxxxxxxxxxxxx"

# Log in to Salesforce
sf = Salesforce(username=USERNAME, password=PASSWORD, security_token=SECURITY_TOKEN) ## , instance_url=SF_URL)

# Determine the date one week ago from today
one_week_ago = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Query Salesforce metadata to get a list of all fields for the "Task" object
describe = sf.Task.describe()
field_names = [field['name'] for field in describe['fields']]

# Remove the 'Description' field from the list
if 'Description' in field_names:
    field_names.remove('Description')

## Since last week
# soql_query = f"SELECT {','.join(field_names)} FROM Task WHERE CreatedDate >= {one_week_ago} OR LastModifiedDate >= {one_week_ago}"
# soql_query = f"SELECT {','.join(field_names)} FROM Task order by Days_from_Creation__c limit 50"
soql_query = f"SELECT {','.join(field_names)} FROM Task"

## -------------------
# soql_query = f"SELECT COUNT() FROM Task"  ## limit 50"
# # Execute the query
# record_count = sf.query(soql_query)
# counted = record_count['totalSize']
# print(f"Counted: {counted}")
## -------------------

# # Use the query to retrieve data
records = sf.query_all(soql_query)
all_records = records['records']
# print(f"total record: {len(all_records)}")

# # Remove attributes key from records
for record in all_records:
    record.pop('attributes', None)

# # If you want to save the data to a CSV file
with open('/tmp/tasks.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    for record in all_records:
        writer.writerow(record)

print(f"Retrieved {len(all_records)} tasks from Salesforce.")