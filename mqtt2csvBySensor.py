import random
import time
import json
import csv
import os.path
import re
from datetime import datetime
from paho.mqtt import client as mqtt_client

broker = 'damoa.io'
port = 1883
client_id = f'python-mqtt-{random.randint(0, 100000)}'
TOPIC="s2m1s/#"
FILE='data.csv'

def on_message(client, _t, msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    topic = msg.topic
    if re.match('^s2m1s/1200[01]\d/data$', topic):
        user = topic.split('/')[1]
        dstr=msg.payload.decode("utf8")
        print(ts, msg.topic, dstr)
        d=json.loads(dstr)
        body=[user, ts, d['T'],d['H'],d['Hpa'],d['Alt'],d['CO2'],d['Gas'],d['VOC'],d['Nox'],d['PM25'],d['PM10'],d['serial']]
        if not os.path.isfile(FILE):
            with open(FILE, 'a') as f: f.write('Sensor,Time,Temp,Humid,Pressure,Altitude,CO2,Gas,VOC,NOX,PM2.5,PM10,Serial\n')
        with open(FILE, 'a', newline='', encoding='utf8') as f:
            fc=csv.writer(f)
            fc.writerow(body)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            if TOPIC != "":
                client.subscribe(TOPIC)
                print("subscribed to {}".format(TOPIC))
        else:
            print("Failed to connect, return code %d\n", rc)

    def on_disconnect(client, userdata, rc):
        global flag_connected
        print("Disconnected from MQTT Broker!")


    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(broker, port)
    return client

client = connect_mqtt()
client.loop_forever()
