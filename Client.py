# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 13:39:59 2019

@author: judejinjin
"""

import asyncio
import pandas as pd
import numpy as np
import time
import json
import socket
import gzip
import json
import threading
import sys

async def tcp_echo_client(message, loop):
    reader, writer = await asyncio.open_connection('127.0.0.1', 8888,
                                                   loop=loop)
    while True:
        print('Send: %r' % message)
        writer.write(message.encode())

        data = await reader.read(100)
        if not data:
            break
        print('Received: %r' % data.decode())
        time.sleep(1)
        
    print('Close the socket')
    writer.close()
    
async def main():
    global mainEventLoop
    global reader
    global writer
    reader, writer = await asyncio.open_connection('127.0.0.1', 8888, loop=mainEventLoop)

def connect(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    return sock

def insert(sock, t):
    payload = gzip.compress(t.to_json().encode())
    m ='{"cmd": "insert", "table": "bidask", "scope": "global", "format": "json", "size":'  + str(len(payload)) + '}'
    sock.sendall(gzip.compress(m.encode()))
    resp = sock.recv(1024)
    if not resp:
        return "NOK"
    resp = resp.decode()
    if resp != "OK":
        return resp
    sock.sendall(payload)
    resp = sock.recv(1024)
    if not resp:
        return "NOK"
    resp = resp.decode()
    return resp

    
def delete(sock, t):
    m ='{"cmd": "delete", "table": "bidask", "scope": "global", "format": "json", "size": 0}'
    sock.sendall(gzip.compress(m.encode()))
    resp = sock.recv(1024)
    if not resp:
        return "NOK"
    resp = resp.decode()
    return resp

def execute(sock, script):
    payload = gzip.compress(script.encode())
    m ='{"cmd": "execute", "table": "bidask", "scope": "global", "format": "json", "size":'  + str(len(payload)) + '}'
    sock.sendall(gzip.compress(m.encode()))
    resp = sock.recv(1024)
    if not resp:
        return "NOK"
    resp = resp.decode()
    if resp != "OK":
        return resp
    sock.sendall(payload)
    resp = sock.recv(1024)
    if not resp:
        return "NOK"
    resp = resp.decode()
    return resp

def query(sock, query):
    payload = gzip.compress(query.encode())
    m ='{"cmd": "query", "table": "bidask", "scope": "global", "format": "json", "size":'  + str(len(payload)) + '}'
    sock.sendall(gzip.compress(m.encode()))
    resp = sock.recv(1024)
    if not resp:
        return None
    resp = resp.decode()
    if resp != "OK":
        return None
    
    sock.sendall(payload)
    resp = sock.recv(1024)
    resp = json.loads(resp.decode())
    if not resp:
        return None
    status = resp["status"]
    if status == "OK" and resp["size"] > 0:
        dfJson = sock.recv(resp["size"])
        data = gzip.decompress(dfJson).decode()
        df = pd.read_json(data)
        return df
    return None

def PubRecv():
    global subSock
    while True:
        try:
            resp = subSock.recv(1024)
            if not resp:
                break
            try:
                resp = json.loads(resp.decode())
                print(resp)
            except:
                print("PubRecv:" + resp.decode())
        except:
            print("publisher died:" + str(sys.exc_info()[0]))
            break

def subscribe(topic):
    global subSock
    request = '{"topic":"' + topic + '"}'
    subSock.sendall(request.encode());
    
if __name__ == "__main__":
    #mainEventLoop = asyncio.get_event_loop()
    #mainEventLoop.run_until_complete(main())
    
    #message = 'Hello World!'
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(tcp_echo_client(message, loop))
    #loop.close()

    sock = connect('127.0.0.1', 8888)
    N = 100 * 100
    t = pd.DataFrame({
            'pA':[np.random.randint(0, 5, N)],
            'pB':[np.random.randint(0, 5, N)],
            'sA':[np.random.randint(0, 100, N)],
            'sB':[np.random.randint(0, 100, N)]})
    print(insert(sock, t))
    #print(delete(sock, 'bidask'))
    #print(execute(sock, 'root.pop("bidask")'))
    print(query(sock, 'root["bidask"].query("pA>pB")'))
    
    subSock = connect('127.0.0.1', 8889)
    subThread = threading.Thread(target=PubRecv)
    subThread.daemon = True
    subThread.start()
    print("subscribing to test")
    subscribe("test")
    pubCmd = '{"cmd": "publish", "topic": "test", "value": 99}'
    sock.sendall(gzip.compress(pubCmd.encode()))
    pubCmd = '{"cmd": "publish", "topic": "test", "value": 199}'
    sock.sendall(gzip.compress(pubCmd.encode()))
    