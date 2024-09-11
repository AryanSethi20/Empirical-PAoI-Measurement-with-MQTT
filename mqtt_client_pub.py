import random, time, datetime, json, os
import numpy as np
from paho.mqtt import client as mqtt_client
from threading import *

broker = "broker.emqx.io"
port = 1883
status_update_topic = "artc/status_update"
ack_topic = "artc/ack"
ZW_policy = True
ack_flag = Event()  # Flag variable for communications between threads
ZW_policy_flag = Event()  # Flag variable for communications between threads
lamb = 3  # Average number of updates
mu = 3  # Service time of updates
config_dirpath = "./config"
config_filename = "config_pub.json"

# Generate a Client ID with the publish prefix.
client_id = f"publish-{random.randint(0, 1000)}"

# Date and time format
dateFormat = "%Y-%m-%d"
timeFormat = "%H-%M-%S.%f"


##
# This function handles the callback when the broker reponds to the client's MQTT connection request.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(
            datetime.datetime.now().strftime(dateFormat + "|" + timeFormat)
            + ": Connected to MQTT Broker with topic "
            + status_update_topic
        )
    else:
        print(
            datetime.datetime.now().strftime(dateFormat + "|" + timeFormat)
            + ": Failed to connect, return code {:n}".format(rc)
        )


##
# This function handles the callback when a message has been received on a topic that the client subscribes to.
def on_message(client, userdata, msg):
    if ZW_policy_flag.is_set():
        print(
            datetime.datetime.now().strftime(dateFormat + "|" + timeFormat)
            + ": Received "
            + msg.payload.decode()
            + " from "
            + msg.topic
        )

        # Set ack_flag to True when an ACK is received from the subscriber client
        ack_flag.set()


##
# This function handles the publishing of status updates to status_update_topic
def publish_status_update(client, status_update, topic, idx):
    # Publish status update to status_update_topic
    result = client.publish(topic, status_update)
    status = result[0]
    if status == 0:
        print(
            datetime.datetime.now().strftime(dateFormat + "|" + timeFormat)
            + ": Published status update index {:n}".format(idx)
        )
    else:
        print(
            datetime.datetime.now().strftime(dateFormat + "|" + timeFormat)
            + ": Failed to publish status update to "
            + topic
        )


##
# This function publishes status updates on MQTT under the ZW or CU policy
def policy_daemon(client: mqtt_client):
    idx = 1
    while True:
        status_update = json.dumps(
            {
                "generation_time": time.time(),
                "mu": mu,
                "lamb": lamb,
                "idx": idx,
            }
        )

        # Publish status update to status_update_topic
        publish_status_update(client, status_update, status_update_topic, idx)

        if ZW_policy_flag.is_set():
            # Await ACK from the subscriber client, timeout after 10 seconds
            ack_flag.wait(10)
            ack_flag.clear()

        # Simulate interarrival delay
        time.sleep(lamb)  # sensor sends data at periodic intervals
        idx += 1


##
# This function runs the main function
def main():
    # Initialise and start MQTT connection
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)

    # Subscribe to ack_topic
    client.subscribe(ack_topic)
    client.on_message = on_message

    # Start a new thread to process network traffic
    client.loop_start()
    policy_daemon(client)

    # Stop the network thread previously created with loop_start().
    client.loop_stop()


if __name__ == "__main__":
    # Initialise config file directory
    if not os.path.exists(config_dirpath):
        os.mkdir(config_dirpath)

    # Initialise config file if it does not exists
    if not os.path.exists(config_dirpath + "/" + config_filename):
        # Write default configurations to config file
        config_dict = {
            "broker": broker,
            "port": port,
            "status_update_topic": status_update_topic,
            "ack_topic": ack_topic,
            "ZW_policy": ZW_policy,
            "config_dirpath": config_dirpath,
            "config_filename": config_filename,
            "lamb": lamb,
            "mu": mu,
        }

        with open(config_dirpath + "/" + config_filename, "w") as config:
            json.dump(config_dict, config, indent=4)

    # Read config file if it exists
    else:
        # Open config file
        with open(config_dirpath + "/" + config_filename, "r") as config:

            # config file
            config_dict = json.load(config)
            broker = config_dict["broker"]
            port = config_dict["port"]
            status_update_topic = config_dict["status_update_topic"]
            ack_topic = config_dict["ack_topic"]
            ZW_policy = config_dict["ZW_policy"]
            config_dirpath = config_dict["config_dirpath"]
            config_filename = config_dict["config_filename"]
            lamb = config_dict["lamb"]
            mu = config_dict["mu"]

            # Initialise ZW_policy_flag
            if ZW_policy:
                status_update_topic = status_update_topic + "/ZW"
                ZW_policy_flag.set()
            else:
                status_update_topic = status_update_topic + "/CU"

    service_times = np.arange(1, 5.1, 0.5)

    for service_time in service_times:
        mu = service_time
        main()
