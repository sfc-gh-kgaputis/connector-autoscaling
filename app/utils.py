import os
from datetime import datetime
from zoneinfo import ZoneInfo


def get_environment_variable(var_name, required=True, default=None):
    """
    Retrieves an environment variable.

    Args:
    var_name (str): The name of the environment variable.
    required (bool): Whether the environment variable is required.
    default: The default value to return if the variable is not found and not required.

    Returns:
    The value of the environment variable or the default value.

    Raises:
    EnvironmentError: If the variable is required but not found.
    """
    value = os.getenv(var_name)
    if value is None:
        if required:
            raise EnvironmentError(f"Required environment variable '{var_name}' not found.")
        return default
    return value


def convert_utc_to_local(utc_time_str, local_zone):
    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_zone = ZoneInfo("UTC")
    local_zone_info = ZoneInfo(local_zone)
    utc_datetime = utc_time.replace(tzinfo=utc_zone)
    local_datetime = utc_datetime.astimezone(local_zone_info)
    return local_datetime


def convert_bytes_to_mb(bytes_value):
    return bytes_value / (1000 * 1000)


# TODO consider linear weighted average if simple average is "too slow"
# TODO add logging
def calculate_average_throughput(data):
    # Initialize a dictionary to keep track of the total MB/sec and count for each topic
    totals = {}

    # Check if there are at least 2 data points
    if len(data) >= 2:
        # Get the most recent and next most recent data points
        most_recent = data[-1]
        next_most_recent = data[-2]

        # Compare the most recent and next most recent data points
        if most_recent['value'] < next_most_recent['value']:
            # Exclude the most recent data point if it's less than the next most recent
            # We want to scale up aggressively, but we don't want to scale down aggressively
            filtered_data = data[:-1]
        else:
            # Include all data points if the most recent is greater than or equal to the next most recent
            filtered_data = data
    else:
        # If there is only 1 data point or less, use all data points
        filtered_data = data

    for entry in filtered_data:
        topic = entry['metric.topic']
        bytes_value = entry['value']
        # Convert bytes to MB
        mb_value = convert_bytes_to_mb(bytes_value)
        # Convert MB to MB/sec (assuming each entry represents one minute)
        mb_per_sec = mb_value / 60

        if topic in totals:
            totals[topic]['total_mb_sec'] += mb_per_sec
            totals[topic]['count'] += 1
        else:
            totals[topic] = {'total_mb_sec': mb_per_sec, 'count': 1}

    # Calculate the weighted average MB/sec for each topic
    averages = {}
    for topic, metrics in totals.items():
        average_mb_sec = metrics['total_mb_sec'] / metrics['count']
        averages[topic] = average_mb_sec

    return averages
