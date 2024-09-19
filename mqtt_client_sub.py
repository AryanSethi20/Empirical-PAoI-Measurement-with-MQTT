import random, time, datetime, json, os
import numpy as np
from paho.mqtt import client as mqtt_client
from threading import *
from scipy import io as spio
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

broker = 'broker.emqx.io'
port = 1883
status_update_topic = "artc/status_update"
ack_topic = "artc/ack"
ZW_policy = True
ack_flag = Event() # Flag variable for communications between threads
measurement_completed_flag = Event() # Flag variable for communications between threads
ZW_policy_flag = Event() # Flag variable for communications between threads
config_dirpath = "./config"
empirical_dirpath = "./Results/empirical_results_lambda=1"
config_filename = "config_sub.json"
log_filename = "PAoI.txt"
mat_filename = 'PAoI.mat'
service_time_filename = 'ServiceTime.txt'
generation_time = 0
numSamples = int(1e5)
minSamples = int(1e3)

# Date and time format
dateFormat = '%Y-%m-%d'
timeFormat = '%H-%M-%S.%f'

# Constants for lambda for simulation purpose
addition_arrival_time_simulation = 9

# Generate a Client ID with the subscribe prefix.
def create_client_id() -> str:
    return f'subscribe-{random.randint(0, 100)}'

##
# This function handles the callback when the broker reponds to the client's MQTT connection request.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Connected to MQTT Broker with topic " + status_update_topic)
    else:
        print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Failed to connect, return code {:n}".format(rc))

##
# This function handles the callback when a message has been received on a topic that the client subscribes to.
def on_message(client, userdata, msg):
    global generation_time
    status_update = json.loads(msg.payload)
    idx = status_update["idx"]

    # Delay added to simulate service time
    mu = status_update["mu"]
    service_time = np.random.exponential(scale=1/mu)
    # time.sleep(service_time)

    # Measure empirical PAoI
    # Extra time is added to simulate the lambda time
    PAoI_measured = time.time() - generation_time + service_time + addition_arrival_time_simulation
    
    # Log empirical PAoI if it is positive, i.e., measured PAoI is not affected by timing mismatch between publisher and subscriber
    if PAoI_measured >= 0:
        with open(empirical_dirpath + "/" + log_filename, 'a') as paoi_logfile:
            paoi_logfile.write(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": {:.4f}\n".format(PAoI_measured))
            paoi_logfile.close()

        # Logs of the service_time for that particular update
        with open(empirical_dirpath + "/" + service_time_filename, 'a') as service_time_logfile:
            service_time_logfile.write(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": {:.4f}\n".format(service_time))
            service_time_logfile.close()
    
        # Stop PAoI measurement if number of samples collected exceed numSamples
        with open(empirical_dirpath + "/" + log_filename, 'r') as paoi_logfile:
            if len(paoi_logfile.readlines()) >= numSamples:
                measurement_completed_flag.set()
            paoi_logfile.close()
            
        print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Status Update Index {:n}\t PAoI: {:.4f}s\t Service Time: {:.4f}s".format(idx, PAoI_measured, service_time))
    
    # Log generation time
    generation_time = status_update["generation_time"]

    # Set ack_flag to True when an ACK is received from the subscriber client under the ZW policy
    if ZW_policy_flag.is_set():
        ack_flag.set()

##
# This function publishes acknowledgement messages for each status update received over MQTT under the ZW policy
def ZW_policy_ack_daemon(client: mqtt_client):
    while True:
        # Await ACK from the subscriber client
        ack_flag.wait(15)
        
        if ZW_policy_flag.is_set():
            # Publish ACK for each status update to ack_topic
            result = client.publish(ack_topic, "ACK")
            status = result[0]
            if status == 0:
                print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Published ACK to " + ack_topic)
            else:
                print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Failed to publish ACK to " + ack_topic)

            ack_flag.clear()
        
        # Stop PAoI measurement if measurement_completed_flag is True
        if measurement_completed_flag.is_set():
            print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": PAoI Measurement Completed")
            break

##
# This function reads the log file and preprocesses the measured PAoI.
def preprocess_paoi_measurements():
    # Read log file and clean up measured PAoI
    PAoI = []
    with open(empirical_dirpath + "/" + log_filename, 'r') as paoi_logfile:
        for line in paoi_logfile:
            PAoI.append(float(line.split(":")[1].strip("\n")))
        PAoI.pop(0)
        paoi_logfile.close()
    
    PAoI = np.array(PAoI[minSamples:])
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Mean PAoI: {:.4f}s".format(np.mean(PAoI)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Variance: {:.4f}".format(np.var(PAoI)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Std Dev: {:.4f}".format(np.std(PAoI)))

    spio.savemat(empirical_dirpath + "/" + mat_filename,\
        {
            'PAoI': PAoI
        })

def plot_mean_PAoI_vs_mean_service_time():
    mean_paoi_cu = []
    mean_paoi_zw = []
    for idx in range(1,10):
        numerical_results = spio.loadmat(empirical_dirpath + f"/CU_PAoI-{idx}.mat")
        PAoI_CU_policy = numerical_results['PAoI'][0]
        
        numerical_results = spio.loadmat(empirical_dirpath + f"/ZW_PAoI-{idx}.mat")
        PAoI_ZW_policy = numerical_results['PAoI'][0]

        mean_paoi_cu.append(np.mean(PAoI_CU_policy))
        mean_paoi_zw.append(np.mean(PAoI_ZW_policy))

    service_times = np.arange(1, 5.1, 0.5)

    # Plot results
    lines = plt.semilogy(
        service_times, mean_paoi_cu,
        service_times, mean_paoi_zw,
    )
    
    plt.setp(lines[0], 'marker', 'o')
    plt.setp(lines[1], 'marker', 's')
    plt.legend(\
        (lines[0], lines[1]), \
        (
            'CU Policy',
            'ZW Policy',
        ), fontsize=8, loc='best')
    
    plt.xlabel(r'Mean Service Time (s)')
    plt.ylabel("Mean PAoI (s)")
    
    ax = plt.gca()
    ax.set_xlim([0.8, 5.5])
    ax.set_ylim([0.1, 3.5])
    ax.set_yticks([float(i) for i in np.arange(0, 3.6, 0.5)])
    ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.ticklabel_format(style='plain', axis='y')

    # plt.ylim(8e-4, 1e-1)
    plt.grid(True, which="both")
    plt.savefig(fname = "./figures/plot_mean_PAoI_vs_mean_service_time.eps", format="eps")
    plt.show()

# This function plots the results for PAoI CDF vs thres
def plot_PAoI_violation_probability_vs_thres():
    # Load results
    numerical_results = spio.loadmat(empirical_dirpath + '/CU_PAoI.mat')
    PAoI_CU_policy  = numerical_results['PAoI'][0]

    numerical_results = spio.loadmat(empirical_dirpath + '/ZW_PAoI.mat')
    PAoI_ZW_policy  = numerical_results['PAoI'][0]

    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Median PAoI CU Policy: {:.4f}s".format(np.median(PAoI_CU_policy)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Median PAoI ZW Policy: {:.4f}s".format(np.median(PAoI_ZW_policy)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Mean PAoI CU Policy: {:.4f}s".format(np.mean(PAoI_CU_policy)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": Mean PAoI ZW Policy: {:.4f}s".format(np.mean(PAoI_ZW_policy)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": PAoI CU Policy Variance: {:.4f}s".format(np.var(PAoI_CU_policy)))
    print(datetime.datetime.now().strftime(dateFormat + "|" +timeFormat) + ": PAoI ZW Policy Variance: {:.4f}s".format(np.var(PAoI_ZW_policy)))

    # Initialise variables
    _min = 0 # tested 0.1
    _max = 0.3 # tested 0.8
    _step = 0.01
    thres = np.arange(start=_min, stop=_max + _step, step=_step)

    PAoI_violation_prob_CU_policy = [len(PAoI_CU_policy[PAoI_CU_policy>thres[i]])/len(PAoI_CU_policy) for i in range(len(thres))]
    PAoI_violation_prob_ZW_policy = [len(PAoI_ZW_policy[PAoI_ZW_policy>thres[i]])/len(PAoI_ZW_policy) for i in range(len(thres))]

    # Plot results
    lines = plt.semilogy(
        thres, PAoI_violation_prob_CU_policy,
        thres, PAoI_violation_prob_ZW_policy,
    )
    plt.setp(lines[0], 'marker', 'o')
    plt.setp(lines[1], 'marker', 's')

    plt.legend(\
        (lines[0], lines[1]), \
        (
            'CU Policy',
            'ZW Policy',
        ), fontsize=8, loc='best')
    
    plt.xlabel(r'Threshold (s)')
    plt.ylabel("PAoI Violation Probability")
    # plt.ylim(8e-4, 1e-1)
    
    plt.grid(True, which="both")
    plt.savefig(fname = "./figures/plot_PAoI_violation_probability_vs_thres.eps", format="eps")
    plt.show()

##
# This function runs the main function
def main():
    # Initialise and start MQTT connection
    client = mqtt_client.Client(create_client_id())
    client.on_connect = on_connect
    client.connect(broker, port)
    client.on_message = on_message

    # Subscribe to status_update_topic
    client.subscribe(status_update_topic)

    # Start a new thread to process network traffic
    client.loop_start()
    ZW_policy_ack_daemon(client)

    # Stop the network thread previously created with loop_start().
    client.loop_stop()
    
    # Preprocess PAoI measurements from log file into mat file
    preprocess_paoi_measurements()
    # plot_PAoI_violation_probability_vs_thres()
    
if __name__ == '__main__':
    # Initialise empirical PAoI logging file directory
    if not os.path.exists(empirical_dirpath):
        os.mkdir(empirical_dirpath)
    
    # Initialise figures directory
    if not os.path.exists("./figures"):
        os.mkdir("./figures")

    # Initialise config file directory and config file
    if not os.path.exists(config_dirpath):
        os.mkdir(config_dirpath)

     # Initialise config file if it does not exists
    if not os.path.exists(config_dirpath+"/"+config_filename):
        # Write default configurations to config file
        config_dict = {
            "broker": broker,
            "port": port,
            "status_update_topic": status_update_topic,
            "ack_topic": ack_topic,
            "ZW_policy": ZW_policy,
            "config_dirpath": config_dirpath,
            "empirical_dirpath": empirical_dirpath,
            "config_filename": config_filename,
            "log_filename": log_filename,
            "mat_filename": mat_filename,
            "numSamples": numSamples,
            "minSamples": minSamples
        }
        
        with open(config_dirpath+"/"+config_filename, "w") as config:
            json.dump(config_dict, config, indent=4)
    
    # Read config file if it exists
    else:
        # Open config file
        with open(config_dirpath+"/"+config_filename, 'r') as config:
        
            # Read config file
            config_dict = json.load(config)

            broker = config_dict["broker"]
            port = config_dict["port"]
            status_update_topic = config_dict["status_update_topic"]
            ack_topic = config_dict["ack_topic"]
            ZW_policy = config_dict["ZW_policy"]
            config_dirpath = config_dict["config_dirpath"]
            empirical_dirpath = config_dict["empirical_dirpath"]
            config_filename = config_dict["config_filename"]
            log_filename = config_dict["log_filename"]
            mat_filename = config_dict["mat_filename"]
            service_time_filename = config_dict["service_time_filename"]
            numSamples = config_dict["numSamples"]
            minSamples = config_dict["minSamples"]
    
    service_times = np.arange(1, 5.1, 0.5)
    
    idx = 9
    while True:
        if idx > len(service_times):
            break
        print(f"========================= {idx} ==============================")
        if idx == 9:
        # if idx == 1:
            if ZW_policy:
                status_update_topic = status_update_topic + "/ZW"
            else:
                status_update_topic = status_update_topic + "/CU"

        if ZW_policy:
            log_filename = "ZW_"
            mat_filename = "ZW_"
            service_time_filename = "ZW_"
            ZW_policy_flag.set()
        else:
            log_filename = "CU_"
            mat_filename = "CU_"
            service_time_filename = "CU_"
        
        log_filename = log_filename + f"PAoI-{idx}.txt"
        mat_filename = mat_filename + f"PAoI-{idx}.mat"
        service_time_filename = service_time_filename + f'ServiceTime-{idx}.txt'
        main()
        measurement_completed_flag.clear()
        idx += 1