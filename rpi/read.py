#!/usr/bin/env python3
import time
import json
import serial
import threading
import re
import requests
import random
from lamp import Lamp
from RepeatedTimer import RepeatedTimer
import paho.mqtt.client as paho
broker="damoa.io"
port=1883
client_id = f'python-mqtt-{random.randint(0, 100000)}'
client=""
user=120021
board={}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("mqtt connected to ", broker)

def on_disconnect(client, userdata, rc):
    print("mqtt disconnected from ", broker)

client= paho.Client(client_id)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.connect(broker,port)

s1=""
s2=""
s3=""

lamp=Lamp()

INTERVAL=1

def open(tty):
    s = serial.Serial(
        port=tty,
        baudrate = 9600,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        timeout=1
    )
    return s

def read_s1():
    tty='/dev/ttyS0'
    global s1
    if s1=="":
        s1=open(tty)
    s1.write(b'[ENQ]')
    d=s1.readline().decode('utf8').strip()
    if not re.match('{"2603":\d+,"ch3sh":\d+}', d):
        print(f"{tty} format fail '{d}'")
        return {}
    try:
        j=json.loads(d)
        print(j)
    #return f'0X{j["2603"]},1X{j["ch3sh"]}'
        return {'ADC':j}
    except:
        print(f'{tty} json fail: {d}')
        return {}

      def read_ek3(s1, usb):
    s1.write(b'[EK3 ECM ENQ]')
    d=s1.readline().decode('utf8').strip()
    if not re.match('\[EK3 ECM .* RLO RAU\]', d):
        print(f"{usb} format fail '{d}'")
        return {}
    d1=d.split()
    #print(d1)
    try:
        j={board[usb]:{'bias_mV':int(d1[2],16), 'ppm_x_100': int(d1[3],16), 'sensor_nA': int(d1[4],16), 'span_ppm_x_100':int(d1[5],16), 'span_nA': int(d1[6],16), 'temperature': float(d1[7])}}
        #print(j)
        return j
    except:
        print(f'{tty} parsing fail: {d1}')
        return {}

def read_s2():
    tty='/dev/ttyUSB0'
    global s2
    if s2=="":
        s2=open(tty)
    if not read_ek3_who(s2, tty): return {}
    return read_ek3(s2, tty)

def read_s3():
    tty='/dev/ttyUSB1'
    global s3
    if s3=="":
        s3=open(tty)
    if not read_ek3_who(s3, tty): return {}
    return read_ek3(s3, tty)

ser=0

def run():
    global ser
    ser+=1

    lamp.on('g')
    d1=read_s1()
    lamp.off('g')

    lamp.on('b')
    d2=read_s2()
    lamp.off('b')

    lamp.on('rg')
    d3=read_s3()
    lamp.off('rg')

    lamp.on('rb')
    #print('d1=', d1,'d2=', d2,'d3=', d3)
    j={**d1, **d2, **d3, "serial":ser}
    ret= client.publish(f"s2m1s/{user}/data", json.dumps(j))
    print(f"s2m1s/{user}/data {j}")
    lamp.off('rb')
    #print(r.text)

client.loop_start()
RepeatedTimer(2, run)
