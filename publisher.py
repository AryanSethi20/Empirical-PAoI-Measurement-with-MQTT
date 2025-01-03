import random, time, datetime, json
import numpy as np
from paho.mqtt import client as mqtt_client
from threading import Event


class PublisherEnvironment:
    def __init__(self):
        self.broker = "broker.emqx.io"
        self.port = 1883
        self.status_update_topic = "artc1/status_update"
        self.ack_topic = "artc1/ack"
        self.policy_topic = "artc1/policy"

        self.lambda_rate = 1.0  # Updates per second for CU
        self.mu_rate = 1.0  # Service rate
        self.total_updates = 0

        # Flags for controlling behavior
        self.ZW_policy_flag = Event()
        self.ack_flag = Event()


class EnhancedMQTTPublisher:
    def __init__(self):
        self.env = PublisherEnvironment()

        # MQTT client setup
        self.client_id = f'publish-{random.randint(0, 1000)}'
        self.client = mqtt_client.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.last_update_time = time.time()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"{self._get_timestamp()}: Connected to MQTT Broker!")
            self.client.subscribe([
                (self.env.ack_topic, 0),
                (self.env.policy_topic, 0)
            ])
        else:
            print(f"{self._get_timestamp()}: Failed to connect, code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            if msg.topic == self.env.policy_topic:
                policy_update = json.loads(msg.payload)
                new_policy = policy_update["policy"]

                if new_policy == "ZW":
                    self.env.ZW_policy_flag.set()
                    self.env.ack_flag.clear()
                else:
                    self.env.ZW_policy_flag.clear()
                    self.env.ack_flag.clear()
                print(f"{self._get_timestamp()}: Switching to {new_policy} policy")

            elif msg.topic == self.env.ack_topic and self.env.ZW_policy_flag.is_set():
                print(f"{self._get_timestamp()}: Received ACK")
                self.env.ack_flag.set()

        except Exception as e:
            print(f"{self._get_timestamp()}: Error processing message: {e}")

    def publish_status_update(self):
        """Publish status update based on current policy"""
        current_time = time.time()
        status_update = {
            "generation_time": current_time,
            "mu": self.env.mu_rate,
            "lambda": self.env.lambda_rate,
            "update_number": self.env.total_updates + 1
        }

        if self.env.ZW_policy_flag.is_set():
            topic = f"{self.env.status_update_topic}/ZW"
        else:
            topic = f"{self.env.status_update_topic}/CU"

        result = self.client.publish(topic, json.dumps(status_update))

        if result[0] == 0:
            self.env.total_updates += 1
            self.last_update_time = current_time
            print(f"{self._get_timestamp()}: Published update {self.env.total_updates}")
            return True
        else:
            print(f"{self._get_timestamp()}: Failed to publish update")
            return False

    def run(self):
        try:
            self.client.connect(self.env.broker, self.env.port)
            self.client.loop_start()

            while True:
                current_time = time.time()

                if self.env.ZW_policy_flag.is_set():
                    # Zero Wait policy - publish and wait for acknowledgment
                    if not self.env.ack_flag.is_set():  # Only send if not waiting for ACK
                        if self.publish_status_update():
                            # Wait for acknowledgment with timeout
                            ack_received = self.env.ack_flag.wait(timeout=5.0)
                            if not ack_received:
                                print(f"{self._get_timestamp()}: ACK timeout")
                            self.env.ack_flag.clear()  # Reset flag after timeout

                else:  # Continuous Update policy
                    if current_time - self.last_update_time >= 1 / self.env.lambda_rate:
                        self.publish_status_update()

                time.sleep(0.01)  # Small sleep to prevent busy waiting

        except KeyboardInterrupt:
            print("\nDisconnecting from broker")
            self.client.loop_stop()
            self.client.disconnect()
            print(f"\nTotal Updates Sent: {self.env.total_updates}")

        except Exception as e:
            print(f"Error: {e}")
            self.client.loop_stop()
            self.client.disconnect()

    def _get_timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d|%H-%M-%S.%f')


if __name__ == "__main__":
    publisher = EnhancedMQTTPublisher()
    publisher.run()