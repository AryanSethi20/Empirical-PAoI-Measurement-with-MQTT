import random, time, datetime, json, os
import numpy as np
from paho.mqtt import client as mqtt_client
from threading import Event
from collections import deque

broker = "broker.emqx.io"
port = 1883
status_update_topic = "artc1/status_update"
ack_topic = "artc1/ack"
policy_topic = "artc1/policy"
window_size = 100  # Samples to consider for policy evaluation
evaluation_threshold = 10  # Minimum samples before allowing policy switch
config_dirpath = "./config"
empirical_dirpath = "./empirical_results"
config_filename = "config_sub.json"
log_filename = "PAoI.txt"
policy_log_filename = "PolicySwitchLog.txt"
num_samples = int(1e5)
min_samples = int(1e3)

class EnhancedMQTTSubscriber:
    def init(self):
    # MQTT Setup
        self.broker = broker
        self.port = port
        self.status_update_topic = status_update_topic
        self.ack_topic = ack_topic
        self.policy_topic = policy_topic

        # Initialize MQTT client
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.client = mqtt_client.Client(self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Initialize flags
        self.ack_flag = Event()
        self.measurement_completed_flag = Event()

        # Policy tracking
        self.window_size = window_size  # Fixed 100-step window
        self.zw_paoi_history = deque(maxlen=self.window_size)
        self.cu_paoi_history = deque(maxlen=self.window_size)
        self.current_policy = "CU"
        self.step_in_window = 0

        # Metrics tracking
        self.generation_time = 0
        self.total_samples = 0

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"{self._get_timestamp()}: Connected to MQTT Broker!")
            self.client.subscribe([
                (self.status_update_topic + "/ZW", 0),
                (self.status_update_topic + "/CU", 0),
                (self.policy_topic, 0)
            ])
        else:
            print(f"{self._get_timestamp()}: Failed to connect, code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            update = json.loads(msg.payload)

            # Handle policy updates
            if msg.topic == self.policy_topic:
                self._handle_policy_update(update)
                return

            # Calculate PAoI
            if "generation_time" in update:
                paoi = time.time() - update["generation_time"]
            else:
                paoi = update.get("paoi", 0)

            # Update appropriate metrics
            policy = update["policy"]
            if policy == "ZW":
                self.zw_paoi_history.append(paoi)
            else:
                self.cu_paoi_history.append(paoi)

            # Log metrics
            self._log_metrics(paoi, policy)

            # Check if it's time to evaluate policies
            self.step_in_window += 1
            if self.step_in_window >= self.window_size:
                self._evaluate_policy()
                self.step_in_window = 0

        except Exception as e:
            print(f"{self._get_timestamp()}: Error processing message: {e}")

    def _log_metrics(self, paoi, policy):
        """Log PAoI metrics to a file."""
        with open(log_filename, "a") as log_file:
            log_file.write(f"{self._get_timestamp()} | Policy: {policy} | PAoI: {paoi}\n")

    def _compute_window_statistics(self):
        """Compute average PAoI for both policies."""
        zw_avg_paoi = sum(self.zw_paoi_history) / len(self.zw_paoi_history) if self.zw_paoi_history else float('inf')
        cu_avg_paoi = sum(self.cu_paoi_history) / len(self.cu_paoi_history) if self.cu_paoi_history else float('inf')
        return zw_avg_paoi, cu_avg_paoi

    def _evaluate_policy(self):
        """Evaluate whether to switch policies based on metrics."""
        zw_avg_paoi, cu_avg_paoi = self._compute_window_statistics()
        new_policy = "ZW" if zw_avg_paoi < cu_avg_paoi else "CU"

        if new_policy != self.current_policy:
            old_policy = self.current_policy
            self.current_policy = new_policy

            # Log policy switch
            self._log_policy_switch(old_policy, new_policy, {
                "ZW Avg PAoI": zw_avg_paoi,
                "CU Avg PAoI": cu_avg_paoi
            })

            # Publish policy update
            self.client.publish(self.policy_topic, json.dumps({"policy": new_policy}))
            print(f"Switched to {new_policy} policy based on metrics.")

    def _log_policy_switch(self, old_policy, new_policy, metrics):
        """Log policy switch details."""
        with open(policy_log_filename, "a") as log_file:
            log_file.write(f"{self._get_timestamp()} | Switched from {old_policy} to {new_policy} | Metrics: {metrics}\n")

    def _get_timestamp(self):
        """Get current timestamp."""
        return datetime.datetime.now().strftime('%Y-%m-%d|%H-%M-%S.%f')

    def run(self):
        try:
            self.client.connect(self.broker, self.port)
            self.client.loop_start()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nDisconnecting from broker")
            self.client.loop_stop()
            self.client.disconnect()

        except Exception as e:
            print(f"Error: {e}")
            self.client.loop_stop()
            self.client.disconnect()

if __name__ == "__main__":
    if not os.path.exists(config_dirpath):
        os.mkdir(config_dirpath)
    
    if not os.path.exists(empirical_dirpath):
        os.mkdir(empirical_dirpath)

    if not os.path.exists(config_dirpath + "/" + config_filename):
        config_dict = {
            "broker": broker,
            "port": port,
            "status_update_topic": status_update_topic,
            "ack_topic": ack_topic,
            "policy_topic": policy_topic,
            "config_dirpath": config_dirpath,
            "empirical_dirpath": empirical_dirpath,
            "config_filename": config_filename,
            "log_filename": log_filename,
            "window_size": window_size,
            "evaluation_threshold": evaluation_threshold,
            "num_samples": num_samples,
            "min_samples": min_samples
        }
        
        with open(config_dirpath + "/" + config_filename, "w") as config:
            json.dump(config_dict, config, indent=4)
    
    else:
        with open(config_dirpath + "/" + config_filename, 'r') as config:
            config_dict = json.load(config)
            broker = config_dict["broker"]
            port = config_dict["port"]
            status_update_topic = config_dict["status_update_topic"]
            ack_topic = config_dict["ack_topic"]
            policy_topic = config_dict["policy_topic"]
            config_dirpath = config_dict["config_dirpath"]
            empirical_dirpath = config_dict["empirical_dirpath"]
            config_filename = config_dict["config_filename"]
            log_filename = config_dict["log_filename"]
            window_size = config_dict["window_size"]
            evaluation_threshold = config_dict["evaluation_threshold"]
            num_samples = config_dict["num_samples"]
            min_samples = config_dict["min_samples"]

    subscriber = EnhancedMQTTSubscriber()
    subscriber.run()