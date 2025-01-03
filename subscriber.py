import random, time, datetime, json
import numpy as np
from paho.mqtt import client as mqtt_client
from threading import Event
from collections import deque


class MQTTEnvironment:
    def __init__(self):
        self.broker = "broker.emqx.io"
        self.port = 1883
        self.status_update_topic = "artc1/status_update"
        self.ack_topic = "artc1/ack"
        self.policy_topic = "artc1/policy"

        self.window_size = 100  # Updates per policy window
        self.current_window_updates = 0
        self.total_updates = 0
        self.paoi_window = deque(maxlen=self.window_size)

        # Control flags
        self.ZW_policy_flag = Event()
        self.ack_flag = Event()


class PolicyManager:
    def __init__(self, env):
        self.env = env
        self.policy_history = []
        self.peak_aoi = {
            "CU": float('-inf'),
            "ZW": float('-inf')
        }
        self.window_count = 0

    def should_switch_policy(self):
        return self.env.current_window_updates >= self.env.window_size

    def switch_policy(self):
        """Switch policies based on window count and peak AoI performance"""
        # Calculate metrics for current window
        if len(self.env.paoi_window) > 0:
            current_peak_aoi = max(self.env.paoi_window)
            current_policy = "ZW" if self.env.ZW_policy_flag.is_set() else "CU"
            self.peak_aoi[current_policy] = max(
                self.peak_aoi[current_policy],
                current_peak_aoi
            )

        # Increment window counter
        self.window_count += 1

        # Determine next policy
        if self.window_count == 1:
            next_policy = "ZW"
            self.env.ZW_policy_flag.set()
        elif self.window_count >= 2:
            if self.peak_aoi["CU"] <= self.peak_aoi["ZW"]:
                next_policy = "CU"
                self.env.ZW_policy_flag.clear()
            else:
                next_policy = "ZW"
                self.env.ZW_policy_flag.set()
        else:
            next_policy = "CU"
            self.env.ZW_policy_flag.clear()

        # Reset for next window
        self.env.current_window_updates = 0
        self.env.paoi_window.clear()
        self.env.ack_flag.clear()

        # Record policy change
        self.policy_history.append({
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d|%H-%M-%S.%f'),
            "new_policy": next_policy,
            "peak_aoi_cu": self.peak_aoi["CU"],
            "peak_aoi_zw": self.peak_aoi["ZW"],
            "window_count": self.window_count,
            "total_updates": self.env.total_updates
        })

        print(f"\nWindow {self.window_count} completed:")
        print(f"Peak AoI - CU: {self.peak_aoi['CU']:.4f}s, ZW: {self.peak_aoi['ZW']:.4f}s")
        print(f"Switching to {next_policy} policy\n")

        return next_policy


class EnhancedMQTTSubscriber:
    def __init__(self):
        self.env = MQTTEnvironment()
        self.policy_manager = PolicyManager(self.env)

        # MQTT client setup
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.client = mqtt_client.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"{self._get_timestamp()}: Connected to MQTT Broker!")
            self.client.subscribe([
                (f"{self.env.status_update_topic}/CU", 0),
                (f"{self.env.status_update_topic}/ZW", 0)
            ])
        else:
            print(f"{self._get_timestamp()}: Failed to connect, code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            # Handle policy updates
            if msg.topic == self.env.policy_topic:
                policy_update = json.loads(msg.payload)
                new_policy = policy_update["policy"]
                if new_policy == "ZW":
                    self.env.ZW_policy_flag.set()
                else:
                    self.env.ZW_policy_flag.clear()
                return

            status_update = json.loads(msg.payload)
            current_time = time.time()

            # Calculate PAoI
            paoi = current_time - status_update["generation_time"]
            self.env.paoi_window.append(paoi)
            self.env.current_window_updates += 1
            self.env.total_updates += 1

            # Get current policy from topic
            current_policy = "ZW" if "/ZW" in msg.topic else "CU"

            # Log update with peak AoI information
            current_window_peak = max(self.env.paoi_window)
            print(f"{self._get_timestamp()}: Update {self.env.current_window_updates}/100 | " +
                  f"PAoI: {paoi:.4f}s | Peak: {current_window_peak:.4f}s | " +
                  f"Policy: {current_policy}")

            # Send ACK for ZW policy
            if "/ZW" in msg.topic:
                print(f"{self._get_timestamp()}: Sending ACK...")
                self.client.publish(self.env.ack_topic, "ACK")

            # Check if we should switch policies
            if self.policy_manager.should_switch_policy():
                new_policy = self.policy_manager.switch_policy()
                self.publish_policy_change(new_policy)

        except Exception as e:
            print(f"{self._get_timestamp()}: Error processing message: {e}")

    def publish_policy_change(self, new_policy):
        """Notify publisher of policy change"""
        policy_message = {
            "policy": new_policy,
            "timestamp": time.time()
        }
        self.client.publish(self.env.policy_topic, json.dumps(policy_message))
        print(f"{self._get_timestamp()}: Published policy change to {new_policy}")

    def run(self):
        try:
            self.client.connect(self.env.broker, self.env.port)
            self.client.loop_start()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nDisconnecting from broker")
            self.client.loop_stop()
            self.client.disconnect()

            # Print final statistics
            print("\nFinal Statistics:")
            print(f"Total Updates Processed: {self.env.total_updates}")
            print(f"Policy Switches: {len(self.policy_manager.policy_history)}")
            print("\nPeak AoI Values:")
            print(f"CU Policy: {self.policy_manager.peak_aoi['CU']:.4f}s")
            print(f"ZW Policy: {self.policy_manager.peak_aoi['ZW']:.4f}s")

        except Exception as e:
            print(f"Error: {e}")
            self.client.loop_stop()
            self.client.disconnect()

    def _get_timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d|%H-%M-%S.%f')


if __name__ == "__main__":
    subscriber = EnhancedMQTTSubscriber()
    subscriber.run()