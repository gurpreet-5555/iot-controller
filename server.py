from flask import Flask, request
import random
import os
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--csvpath", type=str, default='sensorLog.csv', help="Path for sensor data log file")
parser.add_argument("-p", "--port", type=int, default=8000, help="Port to receive sensor data ")
args = vars(parser.parse_args())

# Path to csv file logging sensor data
csvPath = args['csvpath']

app = Flask(__name__)

# Service to receive sensor data from client
@app.route('/log-sensor-data', methods=['POST'])
def home():
    # Check if csv file already exists
    isFilePresent = os.path.isfile(csvPath)

    # Fetching request body containing sensor data
    json = request.json

    # Separating parameters from json body
    timestamp = json['timestamp']
    value = json['value']
    sensor = json['sensor']

    # Randomize response to Success or Failure
    if random.randint(0, 9) % 3 != 0:
        with open(csvPath, 'a', newline='') as file:
            writer = csv.writer(file)
            if not isFilePresent:
                writer.writerow(["Timestamp", "Value", "Sensor"])
                file.flush()
                isFilePresent = os.path.isfile(csvPath)
            writer.writerow([timestamp, value, sensor])
            file.flush()
        return 'Success'
    else:
        return 'Failure'


if __name__ == '__main__':
    app.run(debug=True, port=args['port'])
