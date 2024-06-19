# Confluent Connector Autoscaling Example

## Dependencies
- Confluent CLI (https://docs.confluent.io/confluent-cli/current/install.html)
- Miniconda (or another tool for managing Python environments)

## Confluent credentials

### Create API key for connector API
First login to the Confluent CLI.
```
confluent login --save
```
Then create an API key for accessing cloud API resources.  This is different from the API key used for accessing the Kafka Cluster.
```
confluent api-key create --resource cloud
```
You will need this API key and secret to populate the following environment variables:
- `CONFLUENT_CLOUD_API_KEY`
- `CONFLUENT_CLOUD_API_SECRET`

### Create metrics service account and API key
First create a service account for accessing Metrics API.  Take note of the user ID of this service account, as it will be needed in the next few steps.
```bash
confluent iam service-account create <Service Account Name> --description "A service account to read Confluent Cloud metrics"
```
Now create a role binding for the user ID of the service account.
```bash
confluent iam rbac role-binding create --role MetricsViewer --principal User:<Service Account ID>
```
Lastly, create an API key for this service account.
```bash
confluent api-key create --resource cloud --service-account <Service Account ID>
```
You will need this API key and secret to populate the following environment variables:
- `CONFLUENT_METRICS_API_KEY`
- `CONFLUENT_METRICS_API_SECRET`

## Prepare conda environment

```bash
conda create -n ccloud python=3.10
conda activate ccloud
pip install -r requirements.txt
```

## Set required environment variables
The following environment variables are used to configure the connector autoscaler script.
```bash
export CONFLUENT_CLOUD_API_KEY="<redacted>"
export CONFLUENT_CLOUD_API_SECRET="<redacted>"
export CONFLUENT_METRICS_API_KEY="<redacted>"
export CONFLUENT_METRICS_API_SECRET="<redacted>"
export CONFLUENT_ENVIRONMENT_ID="<Confluent environment ID>"
export CONFLUENT_CLUSTER_ID="<Confluent cluster ID>"
export SNOWFLAKE_CONNECTOR_NAME="<Name of Snowflake sink connector>"
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
### Metrics API
- Confluent Docs - Metrics API: https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
- Metrics API Reference: https://api.telemetry.confluent.cloud/docs
- Metrics API Available Metrics: https://api.telemetry.confluent.cloud/docs/descriptors/datasets/cloud

### Connector API
- Confluent Docs - Kafka Connect REST API: https://docs.confluent.io/platform/current/connect/references/restapi.html
- Connector API Course: https://developer.confluent.io/courses/kafka-connect/connect-api-hands-on/
