import requests
from requests.auth import HTTPBasicAuth

from app.runtime_config import get_metrics_api_key, get_metrics_api_secret, get_confluent_cluster_id
from app.utils import calculate_average_throughput


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


def get_received_bytes_metrics():
    # TODO make topic configurable, and support multiple topics
    kafka_topic = "game_events"
    query_json = {
        "aggregations": [
            {
                "metric": "io.confluent.kafka.server/received_bytes",
                "agg": "SUM"
            }
        ],
        "filter": {
            "op": "AND",
            "filters": [
                {
                    "field": "resource.kafka.id",
                    "op": "EQ",
                    "value": get_confluent_cluster_id()
                },
                {
                    "field": "metric.topic",
                    "op": "EQ",
                    "value": f"{kafka_topic}"
                }
            ]

        },
        "granularity": "PT1M",  # look at 1 minute intervals
        "group_by": [
            "metric.topic"
        ],
        "intervals": [
            "PT15M/now"  # over the last 15 minutes
        ],
        "limit": 25
    }
    print(f"Using query_json: {query_json}")
    response = requests.post(build_metrics_api_query_url(), headers=build_metrics_api_headers(),
                             auth=build_metric_api_auth(), json=query_json)
    if response.status_code != 200:
        print(f"Received error response: {response.json()}")
        raise RuntimeError(f"Unable to get obtain received_bytes_metrics, response status: {response.status_code}")
    return response.json()


def calculate_kafka_throughput_mb_sec():
    # TODO make sure we actually got data
    json_response = get_received_bytes_metrics()
    # TODO further restrict lookback if query brought in too much data (e.g. only consider 10 minutes instead of 15)
    averages = calculate_average_throughput(json_response['data'])
    print(f"Calculated topic-level average throughput: {averages}")
    total_throughput = sum(averages.values())
    print(f"Calculated total throughput: {total_throughput}")
    return total_throughput


if __name__ == "__main__":
    calculate_kafka_throughput_mb_sec()
