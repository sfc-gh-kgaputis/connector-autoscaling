import requests
from requests.auth import HTTPBasicAuth

from app.runtime_config import get_cloud_api_key, get_cloud_api_secret, get_confluent_environment_id, \
    get_confluent_cluster_id, get_snowflake_connector_name


def build_connector_api_headers():
    headers = {
        'Content-Type': 'application/json',
    }
    return headers


def build_connector_api_auth():
    cloud_api_key = get_cloud_api_key()
    cloud_api_secret = get_cloud_api_secret()
    return HTTPBasicAuth(cloud_api_key, cloud_api_secret)


def build_connector_api_base_url():
    environment_id = get_confluent_environment_id()
    cluster_id = get_confluent_cluster_id()
    return f"https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{cluster_id}/connectors"


def build_connector_api_config_url(connector_name):
    return f"{build_connector_api_base_url()}/{connector_name}/config"


def get_connector_config(connector_name):
    print(f"Getting config for: {connector_name}")
    response = requests.get(build_connector_api_config_url(connector_name), headers=build_connector_api_headers(),
                            auth=build_connector_api_auth())
    if response.status_code != 200:
        print(f"Received error response: {response.json()}")
        raise RuntimeError(f"Unable to get connector config, response status: {response.status_code}")
    return response.json()


def update_connector_config(connector_name, new_config):
    print(f"Updating config for: {connector_name}, new_config: {new_config}")
    response = requests.put(build_connector_api_config_url(connector_name), headers=build_connector_api_headers(),
                            auth=build_connector_api_auth(), json=new_config)
    if response.status_code != 200:
        print(f"Received error response: {response.json()}")
        raise RuntimeError(f"Unable to update connector config, response status: {response.status_code}")


def main():
    try:
        result = get_connector_config(get_snowflake_connector_name())
        print(f"API call for get_connector_config() returned: {result}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
