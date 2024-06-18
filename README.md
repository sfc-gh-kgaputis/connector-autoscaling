# Confluent Connector Autoscaling

## Dependencies
- Confluent CLI (https://docs.confluent.io/confluent-cli/current/install.html)

## Prepare conda environment

```bash
conda create -n ccloud python=3.10
conda activate ccloud
pip install -r requirements.txt
```

## Run autoscaler locally

```bash
PYTHONPATH=. python app/kafka_connect_autoscaler.py
```

## Test API clients

```bash
PYTHONPATH=. python app/confluent_metrics_api_client.py
PYTHONPATH=. python app/confluent_connector_api_client.py
```

## Resources
Metrics API:
- Confluent Docs - Metrics API: https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
- Metrics API Reference: https://api.telemetry.confluent.cloud/docs
- Metrics API Available Metrics: https://api.telemetry.confluent.cloud/docs/descriptors/datasets/cloud

Connector API:
- Confluent Docs - Kafka Connect REST API: https://docs.confluent.io/platform/current/connect/references/restapi.html
- Connector API Course: https://developer.confluent.io/courses/kafka-connect/connect-api-hands-on/
