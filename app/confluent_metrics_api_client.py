import requests
from requests.auth import HTTPBasicAuth

from app.utils import get_environment_variable


def get_metrics_api_key():
    return get_environment_variable("CONFLUENT_METRICS_API_KEY", True)


def get_metrics_api_secret():
    return get_environment_variable("CONFLUENT_METRICS_API_SECRET", True)


def get_confluent_cluster_id():
    return get_environment_variable("CONFLUENT_CLUSTER_ID", True)


def build_metrics_api_headers():
    headers = {
        'Content-Type': 'application/json',
    }
    return headers


def build_metric_api_auth():
    metrics_api_key = get_metrics_api_key()
    metrics_api_secret = get_metrics_api_secret()
    return HTTPBasicAuth(metrics_api_key, metrics_api_secret)


def build_metrics_api_query_url():
    return 'https://api.telemetry.confluent.cloud/v2/metrics/cloud/query'


def get_received_bytes():
    query_json = {
        "aggregations": [
            {
                "metric": "io.confluent.kafka.server/received_bytes"
            }
        ],
        "filter": {
            "field": "resource.kafka.id",
            "op": "EQ",
            "value": get_confluent_cluster_id()
        },
        "granularity": "PT1H",
        "group_by": [
            "metric.topic"
        ],
        "intervals": [
            "PT1M/now"
        ],
        "limit": 25
    }
    print(f"Using query_json: {query_json}")
    response = requests.post(build_metrics_api_query_url(), headers=build_metrics_api_headers(),
                             auth=build_metric_api_auth(), json=query_json)
    return response.json()


if __name__ == "__main__":
    result = get_received_bytes()
    print(f"API call for get_received_bytes() returned: {result}")
