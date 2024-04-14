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

# payload = { "email": "igal.emona@gmail.com", "fields" : { "first_name": "Igal is the KING!", "country": "XXXX" } }
payload = { "email": "igal.emona@gmail.com", "account_id": "4034555", "fields" : { "address": "Somewhere", "OS": "Windows" } }


## update contact url [by email]
url = 'https://api.ongage.net/214528/api/v2/contacts'

# Make the API request
response = requests.put(url, headers=headers, json=payload)


# Print the response
print(f'Response: \n{response.json()}')
