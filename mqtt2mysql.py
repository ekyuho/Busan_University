import random
import time
import json
import signal
import numpy as np
import os
import sys
import requests
from RepeatedTimer import RepeatedTimer
from datetime import datetime
from paho.mqtt import client as mqtt_client

broker = 'damoa.io'
port = 1883
client_id = f'python-mqtt-{random.randint(0, 100000)}'
TOPIC="s2m1s/#"

# pip install pymysql-pooling
from pymysqlpool.pool import Pool

pool=Pool(host='YOUR MYSQL DATABASE SERVER',
            user='YOUR ACCOUNT',
            password='YOUR PASSWORD',
            db='connware',
            autocommit=True,
            charset='utf8')
pool.init()

def alarm(msg):
    return
    channel = ''
    r=requests.post(channel, json={"text":msg})
    if not r.status_code == 200:
        print('alarm got {r.status_code}')

def insert(q):
    #print(q)
    try:
        con=pool.get_conn()
        cur=con.cursor()
        cur.execute(q)
        pool.release(con)
    except:
        print(f"*** err {q}")
        return
    #print(f'inserted {cur.rowcount}')


watchdog={}
received={}

timer1=''
timer2=''

def sigint_handler(signal, frame):
    timer1.stop()
    timer2.stop()
    print('cleared all thread')
    sys.exit()
signal.signal(signal.SIGINT, sigint_handler)


def do_1sec():
    ts = datetime.now()
    for u in watchdog:
        passed=(ts-watchdog[u]).total_seconds()
        if passed>10:
            print(f'watch dog expiration: {u} {passed:.0f}')

def do_15sec():
    r=f'watchdog(1200): {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} '
    for x in sorted(watchdog):
        r+=f'{x-120000}:-{(datetime.now()-watchdog[x]).total_seconds():.1f}s '
    print(r)

def do_1min():
    ts = datetime.now()
    print('\n')
    print('***** at one minute', ts)


    def stype(d):
        if d=='T': return 'T',0
        if d=='H': return 'H',0
        if d=='Hpa': return 'P',0
        if d=='Alt': return 'A',0
        if d=='CO2': return 'C',0
        if d=='Gas': return 'V',0
        if d=='VOC': return 'V',1
        if d=='Nox': return 'N',0
        if d=='PM25': return 'D',0
        if d=='PM10': return 'D',1

    job='insert into sensors (user,serial,value,type,user2,time,ip) values'
    comma=''
    global received
    for u in received:
        data=received[u]  # received[u]=[d,,,,]    d['user'],d['time'],d['T'],d['H'],d['Hpa'],d['Alt'],d['CO2'],d['Gas'],d['VOC'],d['Nox'],d['PM25'],d['PM10'],d['serial']
        try:
            r=requests.get(f'http://damoa.io:8080/set_received?u={u}&s={data["serial"]}')
            print(f'http://damoa.io:8080/set_received?u={u}&s={data["serial"]}')
            if not r.status_code == 200:
                print('got code ', r.status_code)
        except:
            print('got error in request, try later')

        for d in data:
            if d=='serial': continue
            value = np.percentile(np.array(data[d]), 50)
            print(d, value, min(data[d]),'..',max(data[d]))
            t,u2=stype(d)
            job += f"{comma}({u},{data['serial']},{value},'{t}',{u2},'{ts.strftime('%Y-%m-%d %H:%M:%S')}','mqtt')"
            comma=','
            data[d]=[]


    print(f'{job[:25]}...{job[-25:]}')
    insert(job)

def on_message(client, _t, msg):
    ts = datetime.now()
    topic = msg.topic
    if not topic.startswith('s2m1s/1200'): return
    #if topic.startswith('s2m') and '/data' not in topic and not topic.endswith('stat'):
    user = int(topic.split('/')[1])
    dstr=msg.payload.decode("utf8")
    #print(ts, msg.topic, dstr)
    d=json.loads(dstr)
    d['time']=ts  #.strftime('%Y-%m-%d %H:%M:%S')
    d['user']=user
    #body=[d['user'],d['time'],d['T'],d['H'],d['Hpa'],d['Alt'],d['CO2'],d['Gas'],d['VOC'],d['Nox'],d['PM25'],d['PM10'],d['serial']]
    global watchdog
    watchdog[d['user']]=ts
    global received
    if d['user'] not in received: received[d['user']]={'serial':d['serial'],'T':[],'H':[],'Hpa':[],'Alt':[],'CO2':[],'Gas':[],'VOC':[],'Nox':[],'PM25':[],'PM10':[]}
    received[d['user']]['T'].append(d['T'])
    received[d['user']]['H'].append(d['H'])
    received[d['user']]['Hpa'].append(d['Hpa'])
    received[d['user']]['Alt'].append(d['Alt'])
    received[d['user']]['CO2'].append(d['CO2'])
    received[d['user']]['Gas'].append(d['Gas'])
    received[d['user']]['VOC'].append(d['VOC'])
    received[d['user']]['Nox'].append(d['Nox'])
    received[d['user']]['PM25'].append(d['PM25'])
    received[d['user']]['PM10'].append(d['PM10'])
    #print(len(received[d['user']]['T']), end=' ',flush=True)

   
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
timer1=RepeatedTimer(1, do_1sec)
timer1=RepeatedTimer(15, do_15sec)
timer2=RepeatedTimer(60, do_1min)
alarm(f'***** BUSAN University Mqtt 센서 게이트웨이: START AT {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
client.loop_forever()
