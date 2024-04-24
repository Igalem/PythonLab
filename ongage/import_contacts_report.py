import tempfile
import os
from google.cloud import bigquery
from google.cloud import storage


PROJECT_ID = 'tangome-staging'

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

def query_contacts(project_id=PROJECT_ID, og_bulk_size=OG_BULK_SIZE):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define BigQuery query
    query = '''
        SELECT 'igal.emona@gmail.com' AS email,
            '4034555' AS account_id,
            'True' AS sent_free_gift,
            'False' AS sent_gift,
            0 AS free_gifts_count,
            0 AS paid_gifts_count,
            76 AS gift_amount_in_coins,
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
                281 AS gift_amount_in_coins,
                99 AS purchase_count,
                26.5 AS purchase_amount,
                '11/03/2023' AS last_active_date,
                '09/01/2024' AS last_purchase_date,
                15 AS diamonds_earned
    '''
    query_process = """
    WITH og_contacts AS (
        SELECT email, CAST(account_id AS INT64) AS account_id
        FROM ods.ongage_contacts
        WHERE NULLIF(account_id, '') IS NOT NULL
    ),
    account_id_hourly AS (
        SELECT DISTINCT account_id
        FROM tangome-production.tango_sl.sl_account_hourly_metrics 
        WHERE created_ts >= TIMESTAMP_SUB(current_timestamp, INTERVAL 2 HOUR)	
        AND account_id IS NOT NULL
    )
    ----------- Main query
    SELECT 
        c.email, 
        lfm.account_id, 
        view_duration_min AS view_time,
        CASE WHEN COALESCE(number_of_free_gifts_sent, 0) > 0 THEN TRUE ELSE FALSE END AS sent_free_gift,
        CASE WHEN COALESCE(number_of_paid_gifts_sent, 0) > 0 THEN TRUE ELSE FALSE END AS sent_gift,
        COALESCE(number_of_free_gifts_sent, 0) AS free_gifts_count,
        COALESCE(number_of_paid_gifts_sent, 0) AS paid_gifts_count,
        COALESCE (number_of_free_gifts_received, 0) + COALESCE(number_of_paid_gifts_received, 0) AS gift_amount_in_coins, 
        COALESCE(purchases_count, 0) AS purchase_count,
        COALESCE(purchases_usd, 0) AS purchase_amount,
        FORMAT_DATE('%d/%m/%Y', last_seen) AS last_active_date,
        FORMAT_DATE('%d/%m/%Y', last_purchase_date) AS last_purchase_date,
        number_of_streams AS bc_count,
        stream_duration_min AS bc_time,
        COALESCE(diamonds_addition_free, 0) + COALESCE(diamonds_addition_paid, 0) AS diamonds_earned
    FROM tangome-production.tango_sl.mv_account_lifetime_metrics lfm
    JOIN og_contacts AS c
    ON c.account_id = lfm.account_id
    JOIN account_id_hourly AS ah
    ON c.account_id = ah.account_id
    --WHERE c.email = 'isubigorsmtn@gmail.com'
    limit 100
    """
    # Execute the query
    query_job = client.query(query_process)
    query_job.result()

    return query_job

def upload_file_to_gcs(bucket_name, source_file_path, csv_filename, project_id=PROJECT_ID):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(csv_filename)

    # Upload the file
    blob.upload_from_filename(source_file_path)

def upload_temp_csv_to_gcs(results, bucket_name, csv_filename=None, project_id=PROJECT_ID):
    with tempfile.TemporaryDirectory() as tmp:
        filename = 'ongage_contacts_p.csv'
        path = os.path.join(tmp, filename)
        new_line = '\n'
        with open(path, 'a+') as f:
            for i, row in enumerate(results):
                if i==0:
                    csv_columns = ','.join(row.keys())
                    f.write(csv_columns)
                    f.write(new_line)
                
                row_values = row.values()
                values = ','.join(str(item).replace('None', '') for item in row_values)
                f.write(values)
                f.write(new_line)
                
        if not csv_filename:
            file_name = os.path.basename(path)
            csv_filename = file_name
            
        upload_file_to_gcs(bucket_name=bucket_name, source_file_path=path, csv_filename=csv_filename, project_id=project_id)


if __name__ == "__main__":
    query_job = query_contacts(og_bulk_size=OG_BULK_SIZE)

    bucket_name = 'ongage-testing'
    csv_filename = 'ongage_contacts.csv'
    upload_temp_csv_to_gcs(results=query_job.result(), bucket_name=bucket_name)