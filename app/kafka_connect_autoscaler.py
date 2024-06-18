import time
from math import ceil
import random


class KafkaConnectAutoscaler:
    def __init__(self):
        self.max_throughput_per_task = 1  # Maximum throughput per task in MB/sec
        self.upper_threshold = 0.8  # Upper threshold percentage
        self.lower_threshold = 0.6  # Lower threshold percentage
        self.cooldown_period = 20  # Cooldown period in seconds
        self.check_interval = 5  # Interval between autoscaling checks in seconds

        # TODO store in persistent store (e.g. SSM or dynamo)
        self._last_scaling_time = 0  # Timestamp of the last scaling event
        # TODO remove, replace with live count
        self._last_task_count = 0

    def get_current_tasks(self):
        # TODO get tasks from connector API
        if self._last_task_count == 0:
            print(f"Simulated start with 1 task")
            self._last_task_count = 1
        current_tasks = self._last_task_count
        return current_tasks

    # noinspection PyMethodMayBeStatic
    def get_kafka_throughput_mb_sec(self):
        # TODO get throughput from metrics API
        kafka_throughput_mb_sec = random.uniform(0.5, 200)
        print(f"Simulated kafka_throughput_mb_sec: {kafka_throughput_mb_sec}")
        return kafka_throughput_mb_sec

    def scale_tasks(self, desired_tasks):
        # TODO update tasks using connector API
        print(f"Scaling tasks to: {desired_tasks}")
        self._last_task_count = desired_tasks

    def set_last_scaling_time(self, timestamp):
        self._last_scaling_time = timestamp

    def get_last_scaling_time(self):
        return self._last_scaling_time

    def autoscale(self):
        print("Checking if autoscale is required")
        current_tasks = self.get_current_tasks()

        # Get the current Kafka throughput from Confluent Metrics API
        kafka_throughput_mb_sec = self.get_kafka_throughput_mb_sec()

        # Get last time of a scaling event - used to implement cooldown period
        last_scaling_time = self.get_last_scaling_time()

        # Calculate the average throughput per task
        avg_throughput_per_task = kafka_throughput_mb_sec / current_tasks

        # Calculate the desired number of tasks
        desired_tasks = ceil(kafka_throughput_mb_sec / self.max_throughput_per_task)

        # Check if scaling is needed
        if avg_throughput_per_task > self.upper_threshold * self.max_throughput_per_task:
            # Scale up condition met
            if time.time() - last_scaling_time >= self.cooldown_period:
                # Cooldown period has passed, perform scaling
                self.scale_tasks(desired_tasks)
                # Update the last scaling time
                self.set_last_scaling_time(time.time())
            else:
                print("Scaling up skipped due to cooldown period")
        elif avg_throughput_per_task < self.lower_threshold * self.max_throughput_per_task:
            # Scale down condition met
            if time.time() - last_scaling_time >= self.cooldown_period:
                # Cooldown period has passed, perform scaling
                self.scale_tasks(desired_tasks)
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
