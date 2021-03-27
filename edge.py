import csv
import time
import requests
import json
from queue import Queue
import threading
from flask import Flask, make_response
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--csvpath", type=str, help="Path for sensor data input file")
parser.add_argument("-p", "--port", type=int, default=8001, help="Port for status service")
parser.add_argument("-s", "--server", type=str, help="Server path to send data")
parser.add_argument("-d", "--senddelay", type=int, default=60,
                    help="Seconds to wait before sending next sensor data record")
parser.add_argument("-b", "--bufferdelay", type=int, default=5, help="Seconds to wait before sending all buffered data")
args = vars(parser.parse_args())

# Path on server to send sensor data
SERVER_API_PATH = args['server']

# File containing simulated sensor data
sensorDataFile = args['csvpath']

# Queue for buffered sensor data
bufferedData = Queue(maxsize=0)

# Counter for successfully transmitted data
transmitCount = 0


# Function to send sensor data to server
def sendData(in_timestamp, in_value, in_sensor):
    global transmitCount
    payload = {'timestamp': in_timestamp,
               'value': in_value,
               'sensor': in_sensor}

    response = requests.post(url=SERVER_API_PATH, data=json.dumps(payload),
                             headers={'content-type': 'application/json'})

    print(response.content)

    if str(response.content) != "b'Success'":
        bufferedData.put((in_timestamp, in_value, in_sensor))  # Buffer sensor data if not sent successfully
    elif str(response.content) == "b'Success'":
        transmitCount += 1
        print("Successfully sent: {}".format(transmitCount))


# Function to read simulated sensor data from csv
def processDataFromCsv():
    with open(sensorDataFile, "r", newline="") as file:
        reader = csv.DictReader(file)  # create a CSV reader

        while True:
            for row in reader:
                timestamp = row['Timestamp']
                value = row['Value']
                sensor = row['Sensor']

                sendData(timestamp, value, sensor)

                time.sleep(args['senddelay'])  # Wait for specified no. of seconds to send new record

            print("No input data left to read -- Waiting")  # No data left to be read from csv


# Function to send buffered data at once
def processBufferedData():
    if not bufferedData.empty():
        print("Sending {} buffered records.".format(bufferedData.qsize()))
        data = bufferedData.get()
        sendData(data[0], data[1], data[2])


app = Flask(__name__)

# Service to get count of successful and buffered records
@app.route('/transmission-status', methods=['GET'])
def home():
    response = make_response(
        json.dumps({'Successful': transmitCount,
                    'Buffered': bufferedData.qsize()}),
        200,
    )
    response.headers["Content-Type"] = "application/json"
    return response


if __name__ == "__main__":
    serviceThread = threading.Thread(target=app.run, kwargs={'port': args['port']})     # Thread for status service
    mainThread = threading.Thread(target=processDataFromCsv)      # Thread for reading csv and sending data

    serviceThread.start()
    mainThread.start()

    while True:
        processBufferedData()
        time.sleep(args['bufferdelay'])     # Wait for specified no. of seconds to send buffered data
