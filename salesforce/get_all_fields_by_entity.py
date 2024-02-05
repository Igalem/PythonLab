from simple_salesforce import Salesforce
import json

# Salesforce credentials
USERNAME = "xxxxx"
PASSWORD = "xxxxx"
SECURITY_TOKEN = "xxxxxxxxxxxxxxxxxxxxx"
DOMAIN = 'login'  # Use 'test' for sandbox

# Initialize Salesforce connection
sf = Salesforce(username=USERNAME, password=PASSWORD, security_token=SECURITY_TOKEN, domain=DOMAIN)

# Get the field names from the describe call
field_names = [field['name'] for field in sf.Account.describe()['fields']]

# SOQL query to retrieve all fields for User entity
query = "SELECT {} FROM Account where name = 'Philips Electronics - ROW' LIMIT 1".format(','.join(field_names))

# Execute the query
result = sf.query_all(query)
# print(result['records'])

for i,row in enumerate(result['records']):
    print(f"{i} -------------------------")
    print(row)

# Print the result
# print(json.dumps(result, indent=4))