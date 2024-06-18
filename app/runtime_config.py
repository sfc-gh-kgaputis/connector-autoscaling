from app.utils import get_environment_variable


def get_confluent_environment_id():
    return get_environment_variable("CONFLUENT_ENVIRONMENT_ID", True)


def get_confluent_cluster_id():
    return get_environment_variable("CONFLUENT_CLUSTER_ID", True)


def get_cloud_api_key():
    return get_environment_variable("CONFLUENT_CLOUD_API_KEY", True)


def get_cloud_api_secret():
    return get_environment_variable("CONFLUENT_CLOUD_API_SECRET", True)


def get_metrics_api_key():
    return get_environment_variable("CONFLUENT_METRICS_API_KEY", True)


def get_metrics_api_secret():
    return get_environment_variable("CONFLUENT_METRICS_API_SECRET", True)


def get_snowflake_connector_name():
    return get_environment_variable("SNOWFLAKE_CONNECTOR_NAME", True)
