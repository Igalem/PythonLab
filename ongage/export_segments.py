import requests


OG_USERNAME = 'XXXXX'
OG_PASSWORD = 'XXXXX'
OG_ACCOUNT_CODE = 'XXXXX'

# Define headers
headers = {
    'x_username': OG_USERNAME,
    'x_password': OG_PASSWORD,
    'x_account_code': OG_ACCOUNT_CODE
}

payload = {
 "name": "My export",
 "date_format": "mm/dd/yyyy",
 "file_format": "csv",
 "segment_id": [1137994294],
 "status": ['active', 'unjoin-member', 'clicked', 'opened', 'inactive', 'bounced', 'complaint']
}

## create contact search url
url = 'https://api.ongage.net/214528/api/export'
# url ='https://api.ongage.net/214528/api/export/2124824103/retrieve'

# Make the API request
response = requests.post(url, headers=headers, json=payload)
# response = requests.get(url, headers=headers)

# print(response.content)

# Print the response
print(f'Response: \n{response.json()}')
