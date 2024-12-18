import random
import time
import datetime
import json
import numpy as np
import os
from paho.mqtt import client as mqtt_client
from threading import Event

broker = "broker.emqx.io"
port = 1883
status_update_topic = "artc1/status_update"
ack_topic = "artc1/ack"
policy_topic = "artc1/policy"
lambda_rate = 1.0
mu_rate = 1.0
config_dirpath = "./config"
config_filename = "config_pub.json"
num_samples = 1000

class EnhancedMQTTPublisher:
    def __init__(self):
        self.broker = broker
        self.port = port
        self.status_update_topic = status_update_topic
        self.ack_topic = ack_topic
        self.policy_topic = policy_topic
        
        # Initialize MQTT client
        self.client_id = f'publish-{random.randint(0, 1000)}'
        self.client = mqtt_client.Client(self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Initialize flags and state
        self.ack_flag = Event()
        self.current_policy = "CU"
        self.waiting_for_ack = False
        
        # Parameters
        self.lambda_rate = lambda_rate  # Arrival rate
        self.mu_rate = mu_rate  # Service rate
        
        # Simulation tracking
        self.last_zw_time = time.time()  # Last simulated ZW update
        self.last_cu_time = time.time()  # Last simulated CU update
        self.simulated_zw_paoi = 0
        self.simulated_cu_paoi = 0
        self.ack_simulation_delay = 0.1  # Simulated ACK delay for ZW
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"{self._get_timestamp()}: Connected to MQTT Broker!")
            self.client.subscribe(self.ack_topic)
            self.client.subscribe(self.policy_topic)
        else:
            print(f"{self._get_timestamp()}: Failed to connect, code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            if msg.topic == self.policy_topic:
                # Handle policy update message
                policy_update = json.loads(msg.payload)
                new_policy = policy_update["policy"]
                
                if new_policy != self.current_policy:
                    self.current_policy = new_policy
                    print(f"{self._get_timestamp()}: Switching to {new_policy} policy")
                    
            elif msg.topic == self.ack_topic and self.current_policy == "ZW":
                # Handle acknowledgment
                self.ack_flag.set()
                self.waiting_for_ack = False
                
        except Exception as e:
            print(f"{self._get_timestamp()}: Error processing message: {e}")

    def publish_update(self):
        """Publish status update based on current policy"""
        status_update = {
            "generation_time": time.time(),
            "mu": self.mu_rate,
            "lambda": self.lambda_rate,
            "policy": self.current_policy
        }
        
        topic = f"{self.status_update_topic}/{self.current_policy}"
        
        if self.current_policy == "ZW" and self.waiting_for_ack:
            return False
            
        result = self.client.publish(topic, json.dumps(status_update))
        
        if result[0] == 0:
            print(f"{self._get_timestamp()}: Published update with {self.current_policy} policy")
            if self.current_policy == "ZW":
                self.waiting_for_ack = True
                self.ack_flag.clear()
            return True
        else:
            print(f"{self._get_timestamp()}: Failed to publish update")
            return False

    def simulate_other_policy(self):
        """Simulate metrics for non-active policy"""
        current_time = time.time()
        
        if self.current_policy == "CU":
            # Simulate ZW policy
            if not self.waiting_for_ack:
                # Simulate what ZW PAoI would be
                self.simulated_zw_paoi = current_time - self.last_zw_time
                # Simulate ACK delay
                time.sleep(self.ack_simulation_delay)
                self.last_zw_time = current_time
                
                # Send simulated ZW metrics
                self._send_simulated_update("ZW", self.simulated_zw_paoi)
        else:
            # Simulate CU policy
            time_since_last_cu = current_time - self.last_cu_time
            if time_since_last_cu >= (1/self.lambda_rate):
                # Simulate what CU PAoI would be
                self.simulated_cu_paoi = time_since_last_cu
                self.last_cu_time = current_time
                
                # Send simulated CU metrics
                self._send_simulated_update("CU", self.simulated_cu_paoi)

    def _send_simulated_update(self, policy, paoi):
        """Send simulated metrics for the non-active policy"""
        simulation_update = {
            "generation_time": time.time(),
            "mu": self.mu_rate,
            "lambda": self.lambda_rate,
            "policy": policy,
            "paoi": paoi,
            "simulated": True
        }
        
        topic = f"{self.status_update_topic}/{policy}/simulated"
        self.client.publish(topic, json.dumps(simulation_update))

    def publish_update(self):
        """Publish actual update based on current policy"""
        status_update = {
            "generation_time": time.time(),
            "mu": self.mu_rate,
            "lambda": self.lambda_rate,
            "policy": self.current_policy,
            "simulated": False
        }
        
        topic = f"{self.status_update_topic}/{self.current_policy}"
        
        if self.current_policy == "ZW" and self.waiting_for_ack:
            return False
            
        result = self.client.publish(topic, json.dumps(status_update))
        
        if result[0] == 0:
            print(f"{self._get_timestamp()}: Published update with {self.current_policy} policy")
            if self.current_policy == "ZW":
                self.waiting_for_ack = True
                self.ack_flag.clear()
                self.last_zw_time = time.time()
            else:
                self.last_cu_time = time.time()
            return True
        else:
            print(f"{self._get_timestamp()}: Failed to publish update")
            return False

    def run(self):
        try:
            self.client.connect(self.broker, self.port)
            self.client.loop_start()
            
            while True:
                if self.current_policy == "CU":
                    # Continuous Update: publish at regular intervals
                    self.publish_update()
                    # Simulate ZW metrics
                    self.simulate_other_policy()
                    time.sleep(1/self.lambda_rate)
                    
                else:  # Zero Wait policy
                    # Publish and wait for acknowledgment
                    if self.publish_update():
                        # Wait for acknowledgment with timeout
                        ack_received = self.ack_flag.wait(timeout=5.0)
                        if not ack_received:
                            print(f"{self._get_timestamp()}: ACK timeout")
                            self.waiting_for_ack = False
                        # Simulate CU metrics
                        self.simulate_other_policy()
                            
        except KeyboardInterrupt:
            print("\nDisconnecting from broker")
            self.client.loop_stop()
            self.client.disconnect()
            
        except Exception as e:
            print(f"Error: {e}")
            self.client.loop_stop()
            self.client.disconnect()

    def _get_timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d|%H-%M-%S.%f')

if __name__ == "__main__":
    if not os.path.exists(config_dirpath):
        os.mkdir(config_dirpath)

    if not os.path.exists(config_dirpath + "/" + config_filename):
        config_dict = {
            "broker": broker,
            "port": port,
            "status_update_topic": status_update_topic,
            "ack_topic": ack_topic,
            "policy_topic": policy_topic,
            "config_dirpath": config_dirpath,
            "config_filename": config_filename,
            "lambda_rate": lambda_rate,
            "mu_rate": mu_rate,
            "num_samples": num_samples
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
            config_filename = config_dict["config_filename"]
            lambda_rate = config_dict["lambda_rate"]
            mu_rate = config_dict["mu_rate"]
            num_samples = config_dict["num_samples"]

    publisher = EnhancedMQTTPublisher()
    publisher.run()