import os
import requests

def find_whl_files(main_folder):
    for root, dirs, files in os.walk(main_folder):
        for file in files:
            if file.endswith(".whl"):
                whl_file = os.path.join(root, file)
                print(whl_file)
                response = upload_to_artifactory(whl_file, artifactory_url, repository, api_key)
                print(response.status_code, response.text)

def upload_to_artifactory(file_path, artifactory_url, repository, api_key):
    """
    Uploads a wheel file to JFrog Artifactory.

    :param file_path: Path to the .whl file to upload
    :param artifactory_url: Base URL of the Artifactory instance
    :param repository: Name of the target repository in Artifactory
    :param api_key: API key for authentication
    :return: Response object
    """
    with open(file_path, 'rb') as f:
        file_name = file_path.split('/')[-1]
        target_url = f"{artifactory_url}/{repository}/{file_name}"
        
        headers = {
            "X-JFrog-Art-Api": api_key
        }
        
        response = requests.put(target_url, headers=headers, data=f)
    
    return response


# Example usage
if __name__ == "__main__":
    file_path = "/xxxx/xxxx/xxxx/jira-3.5.2-py3-none-any.whl"
    artifactory_url = "https://artifactory.xxxxx-lab.com/artifactory"
    repository = "PyPi-xxx"
    api_key = "xxxxxxxxxxxxxxxxxx"

    response = upload_to_artifactory(file_path, artifactory_url, repository, api_key)
    print(response.status_code, response.text)
