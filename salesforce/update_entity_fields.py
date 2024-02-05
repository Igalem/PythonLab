from simple_salesforce import Salesforce

# Salesforce credentials
username = "xxxxx"
password = "xxxxx"
security_token = "xxxxxxxxxxxxxxxxxxxxx"

# Connect to Salesforce
sf = Salesforce(username=username, password=password, security_token=security_token, domain='test')

# JSON payload
payload = [
    {
        "Account_18_digit_ID__c": "???????",
        "Retention_Dashboard_Risk_Flag__c": "????????",
        "Premium_Platinum__c" : 0
    }
]

# Loop through the payload and update each account
for record in payload:
    account_id = record["Account_18_digit_ID__c"]
    update_fields = {
        "Retention_Dashboard_Risk_Flag__c": record["Retention_Dashboard_Risk_Flag__c"],
        "Premium_Platinum__c": record["Premium_Platinum__c"]
    }

    # Update the account
    sf.Account.update(account_id, update_fields)
    # print(f"Updated Account {account_id} with new Risk Flag.")

print("Update completed.")
