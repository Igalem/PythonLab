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
 "list_id": 214528,
 "file_url": "https://storage.googleapis.com/ongage-testing/og_contactsxxx.csv",
 "csv_delimiter": ",",
 "overwrite": True,
 "ignore_empty": True,
 "send_welcome_message": False
 }

url = 'https://api.ongage.net/214528/api/import'

response = requests.post(url, headers=headers, json=payload)

code = response.json()['payload']
print(response.json())


if 'code' in code:
    raise Exception('Failed to generate contacts import report')


