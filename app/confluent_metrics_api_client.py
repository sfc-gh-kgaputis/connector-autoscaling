import sys

import requests
from requests.auth import HTTPBasicAuth

from app.runtime_config import get_metrics_api_key, get_metrics_api_secret, get_confluent_cluster_id
from app.utils import calculate_average_throughput, parse_topics_string


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


def get_received_bytes_metrics(kafka_topics):
    print(f"Query metrics API for received_bytes for topics: {kafka_topics}")
    # TODO assert at least 1 topic was provided
    # First build a list of topic filters
    topic_filters = []
    for topic in kafka_topics:
        topic_filters.append({
            "field": "metric.topic",
            "op": "EQ",
            "value": f"{topic}"
        })
    # Now build the actual metrics query
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
                    "op": "OR",
                    "filters": topic_filters
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


def calculate_kafka_throughput_mb_sec(kafka_topics):
    # TODO make sure we actually got data
    json_response = get_received_bytes_metrics(kafka_topics)
    print(f"Received raw metrics: {json_response}")
    # TODO further restrict lookback if query brought in too much data (e.g. only consider 10 minutes instead of 15)
    averages = calculate_average_throughput(json_response['data'])
    print(f"Calculated topic-level average throughput: {averages}")
    total_throughput = sum(averages.values())
    print(f"Calculated total throughput: {total_throughput}")
    return total_throughput


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_topics = parse_topics_string(sys.argv[1])
    else:
        print("Please provide a comma-separated list of topics as an argument.")
        sys.exit(1)
    calculate_kafka_throughput_mb_sec(input_topics)
