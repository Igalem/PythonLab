from google.cloud import storage
import os
from git import Repo


# Initialize a GCS client
PROJECT_ID = 'XXXXX'
client = storage.Client(project=PROJECT_ID)

# Name of the GCS bucket
bucket_name = "XXXXX"

# Path to the local Git repository
repo_path = "/Users/iemona/PycharmProjects/airflow"

# Initialize a Git repository object
repo = Repo(repo_path)

# Get a list of staged files
current_branch = repo.active_branch
modified_files = [item.a_path for item in repo.index.diff(f"{current_branch}")]

# Upload each staged file to GCS
for file_name in modified_files:
    # Path to the staged file
    file_path = os.path.join(repo_path, file_name)

    # # Path inside the bucket
    blob_name = os.path.relpath(file_path, repo_path)

    # # Upload the file to GCS
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

    print(f"Uploaded {file_name} to {bucket_name}/{blob_name}")
