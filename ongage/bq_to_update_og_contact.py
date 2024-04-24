import requests
from google.cloud import bigquery

# Set your onGage credentials
OG_USERNAME = 'XXXXX'
OG_PASSWORD = 'XXXXX'
OG_ACCOUNT_CODE = 'XXXXX'
OG_BULK_SIZE = 1000

# Define headers for onGage API
headers = {
    'x_username': OG_USERNAME,
    'x_password': OG_PASSWORD,
    'x_account_code': OG_ACCOUNT_CODE
}


def batch_update_contacts(contact_data):
    # Define the URL for updating contacts
    url = 'https://api.ongage.net/214528/api/v2/contacts'
    # Make the API request to update contacts in batch
    response = requests.put(url, headers=headers, json=contact_data)
    # Print the response
    print(f'Batch Update Response: {response.json()}')


def query_contacts(og_bulk_size=OG_BULK_SIZE):
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define BigQuery query
    query = '''
        SELECT 'igal.emona@gmail.com' AS email,
            '4034555' AS account_id,
            'True' AS sent_free_gift,
            'False' AS sent_gift,
            0 AS free_gifts_count,
            0 AS paid_gifts_count,
            11 AS purchase_count,
            26.5 AS purchase_amount,
            '15/04/2024' AS last_active_date,
            '12/04/2024' AS last_purchase_date,
            24 AS diamonds_earned
        UNION ALL
        SELECT 'testing.igal.emona@gmail.com' AS email,
                '40344034' AS account_id,
                'True' AS sent_free_gift,
                'True' AS sent_gift,
                8 AS free_gifts_count,
                9 AS paid_gifts_count,
                99 AS purchase_count,
                26.5 AS purchase_amount,
                '11/03/2023' AS last_active_date,
                '09/01/2024' AS last_purchase_date,
                15 AS diamonds_earned
    '''

    # Execute the query
    query_job = client.query(query)
    total_query_rows = query_job.result().total_rows
    print(f"Total query results: {total_query_rows}")
    batch_data = []

    # Process query results and prepare data for batch update
    for row in query_job.result():
        email = row.email
        account_id = row.account_id
        sent_free_gift = row.sent_free_gift
        sent_gift = row.sent_gift
        free_gifts_count = row.free_gifts_count
        paid_gifts_count = row.paid_gifts_count
        purchase_count = row.purchase_count
        purchase_amount = row.purchase_amount
        last_active_date = row.last_active_date
        last_purchase_date = row.last_purchase_date
        diamonds_earned = row.diamonds_earned

        payload = {
            "email": email,
            "account_id": account_id,
            "fields": {
                "sent_free_gift": sent_free_gift,
                "sent_gift": sent_gift,
                "free_gifts_count": free_gifts_count,
                "paid_gifts_count": paid_gifts_count,
                "purchase_count": purchase_count,
                "purchase_amount": purchase_amount,
                "last_active_date": last_active_date,
                "last_purchase_date": last_purchase_date,
                "diamonds_earned": diamonds_earned
            }
        }


        # Append contact data to the batch
        batch_data.append(payload)
        # Batch update when the batch size reaches the bulk limit
        if len(batch_data) >= og_bulk_size or total_query_rows < og_bulk_size:
            batch_update_contacts(batch_data)
            # Clear the batch data list for the next batch
            batch_data = []

if __name__ == "__main__":
    query_contacts(og_bulk_size=OG_BULK_SIZE)