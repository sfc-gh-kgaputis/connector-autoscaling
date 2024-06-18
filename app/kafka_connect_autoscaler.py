import time
from math import ceil

from app.confluent_connector_api_client import get_connector_config, update_connector_config
from app.confluent_metrics_api_client import calculate_kafka_throughput_mb_sec
from app.runtime_config import get_snowflake_connector_name


class KafkaConnectAutoscaler:
    def __init__(self):
        # Maximum throughput per task in MB/sec
        # This needs to be calibrated based on compression and testing
        self.max_throughput_per_task = 8.5
        self.upper_threshold = 0.8  # Upper threshold percentage
        self.lower_threshold = 0.6  # Lower threshold percentage
        self.cooldown_period = 300  # Cooldown period in seconds
        self.check_interval = 30  # Interval between autoscaling checks in seconds
        # TODO store in persistent store (e.g. SSM or dynamo)
        self._last_scaling_time = 0  # Timestamp of the last scaling event

    # noinspection PyMethodMayBeStatic
    def get_current_connector_config(self):
        return get_connector_config(get_snowflake_connector_name())

    # noinspection PyMethodMayBeStatic
    def get_kafka_throughput_mb_sec(self):
        return calculate_kafka_throughput_mb_sec()

    # noinspection PyMethodMayBeStatic
    def scale_tasks(self, current_config, desired_tasks):
        print(f"Scaling tasks to: {desired_tasks}, using current_config: {current_config}")
        new_config = current_config.copy()
        new_config['tasks.max'] = str(desired_tasks)
        # TODO determine if API allows partial update of just tasks.max
        update_connector_config(get_snowflake_connector_name(), new_config)

    def set_last_scaling_time(self, timestamp):
        self._last_scaling_time = timestamp

    def get_last_scaling_time(self):
        return self._last_scaling_time

    def autoscale(self):
        print("Checking if autoscale is required")

        # Get current config of Snowflake connector
        current_config = self.get_current_connector_config()
        # Parse current task count
        current_tasks = int(current_config['tasks.max'])

        # Get the current Kafka throughput from Confluent Metrics API
        kafka_throughput_mb_sec = self.get_kafka_throughput_mb_sec()

        # Get last time of a scaling event - used to implement cooldown period
        last_scaling_time = self.get_last_scaling_time()

        # Calculate the average throughput per task
        avg_throughput_per_task = kafka_throughput_mb_sec / current_tasks

        # Calculate the desired number of tasks
        # TODO use upper threshold to build in some headroom...
        desired_tasks = ceil(kafka_throughput_mb_sec / self.max_throughput_per_task)
        if desired_tasks == 0:
            print("Overriding desired_tasks from 0->1")
            desired_tasks = 1

        current_tasks_not_desired = current_tasks != desired_tasks

        # Check if scaling is needed
        if current_tasks_not_desired and avg_throughput_per_task > self.upper_threshold * self.max_throughput_per_task:
            # Scale up condition met
            if time.time() - last_scaling_time >= self.cooldown_period:
                # Cooldown period has passed, perform scaling
                self.scale_tasks(current_config, desired_tasks)
                # Update the last scaling time
                self.set_last_scaling_time(time.time())
            else:
                print("Scaling up skipped due to cooldown period")
        elif current_tasks_not_desired and avg_throughput_per_task < self.lower_threshold * self.max_throughput_per_task:
            # Scale down condition met
            if time.time() - last_scaling_time >= self.cooldown_period:
                # Cooldown period has passed, perform scaling
                self.scale_tasks(current_config, desired_tasks)
                # Update the last scaling time
                self.set_last_scaling_time(time.time())
            else:
                print("Scaling down skipped due to cooldown period")
        else:
            print("No scaling needed")

    def run_in_loop(self):
        print("Starting autoscaler...")
        while True:
            self.autoscale()
            time.sleep(self.check_interval)


def main():
    autoscaler = KafkaConnectAutoscaler()
    autoscaler.run_in_loop()


if __name__ == "__main__":
    main()
