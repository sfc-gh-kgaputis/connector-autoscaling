# Confluent Connector Autoscaling

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