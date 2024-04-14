from google.cloud import bigquery


# ENVIORONMENTS = ['att-1']
ENVIORONMENTS = ['att-1', 'aws-1', 'blg-3', 'bvr-1', 'cha-2', 'fin-1', 'fin-2', 'fin-3', 'flx-1', 'for-1', 'fut-1', 'fut-2', 'gme-1', 'ham-1', 'ham-2', 'inf-1', 'jsh-1', 'mod-1', 'mod-2', 'mod-3', 'orc-1', 'orc-2', 'ord-1', 'pep-1', 'prd-1', 'pup-1', 'rec-1', 'ros-1', 'rtc-1', 'sta-1', 'sta-2', 'sta-3', 'sta-8', 'swt-1', 'tit-1', 'tre-1', 'ups-1', 'ven-1', 'vid-1', 'vid-2', 'wav-1', 'wav-2', 'web-1']

PROJECT_ID = 'XXXXX'
DATASET_ID = 'XXXXX'


def create_dataset_if_not_exists(project_id, dataset_id, env=None, client=None, ):
    
    # Initialize BigQuery client
    if not client:
        client = bigquery.Client(project=project_id)
    
    # Check if dataset already exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except Exception as e:
        # Dataset doesn't exist, create it
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set the location of the dataset
        dataset = client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' created successfully.")

# Instantiate a client
client = bigquery.Client(project=PROJECT_ID)

for env in ENVIORONMENTS:
    env = env.replace('-', '_')
    dataset_id = f"{env}_{DATASET_ID}"

    create_dataset_if_not_exists(project_id=PROJECT_ID, dataset_id=dataset_id)
    
    print(f"{dataset_id}.{env}_moderation_report_log")

    query = f"CREATE OR REPLACE TABLE `XXXXX.{dataset_id}.moderation_report_log`\
        AS SELECT * FROM `XXXXX.moderation_data.moderation_report_log`"
    
    # Execute the daynamic query 
    query_job = client.query(query)
    query_job.result()
    print("Query executed successfully.")
    

