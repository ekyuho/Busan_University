import random
import time
import json
import csv
import os.path
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
    if topic.startswith('s2m1s/1200'): # mine is 1200XX
        #if topic.startswith('s2m') and '/data' not in topic and not topic.endswith('stat'):
        dstr=msg.payload.decode("utf8")
        print(ts, msg.topic, dstr)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            if TOPIC != "":
                client.subscribe(TOPIC)
                print("subscribed to {}".format(TOPIC))
                print('Time,Temp,Humid,Pressure,Altitude,CO2,Gas,VOC,NOX,PM2.5,PM10,Serial')
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
