import requests
from requests.auth import HTTPBasicAuth

from app.utils import get_environment_variable


def get_cloud_api_key():
    return get_environment_variable("CONFLUENT_CLOUD_API_KEY", True)


def get_cloud_api_secret():
    return get_environment_variable("CONFLUENT_CLOUD_API_SECRET", True)


def get_confluent_environment_id():
    return get_environment_variable("CONFLUENT_ENVIRONMENT_ID", True)


def get_confluent_cluster_id():
    return get_environment_variable("CONFLUENT_CLUSTER_ID", True)


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
    return response.json()


def main():
    try:
        result = get_connector_config("snowflake_game_events")
        print(f"API call for get_connector_config() returned: {result}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
