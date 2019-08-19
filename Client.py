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

def connect(ip, port):
    global sock
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    '''
    while True:
        try:
            sock.sendall(bytes(message, 'ascii'))
        except:
            break
        time.sleep(1)
    '''     
        
'''
name: table name
data: data in json format
def insert(name, data):
    global writer, reader
    content = {}
    content['cmd'] = 'insert'
    content['data'] = data
    tosend = json.dumps(content).encode()
    header = 'SU' + tosend.count()
    header = header.encode()
    print(header.count())
    writer.write(header)
    writer.write(tosend)
'''

async def main():
    global mainEventLoop
    global reader
    global writer
    reader, writer = await asyncio.open_connection('127.0.0.1', 8888, loop=mainEventLoop)
    
if __name__ == "__main__":
    #mainEventLoop = asyncio.get_event_loop()
    #mainEventLoop.run_until_complete(main())
    
    #message = 'Hello World!'
    #loop = asyncio.get_event_loop()
    #loop.run_until_complete(tcp_echo_client(message, loop))
    #loop.close()
    connect('127.0.0.1', 8888)
    N = 2 * 2
    t = pd.DataFrame({
            'pA':[np.random.randint(0, 5, N)],
            'pB':[np.random.randint(0, 5, N)],
            'sA':[np.random.randint(0, 100, N)],
            'sB':[np.random.randint(0, 100, N)]})
    print(t.to_json())
    payload = gzip.compress(t.to_json().encode())
    print('payload length is ' + str(len(payload)))
    m ='{"cmd": "insert", "table": "bidask", "size":'  + str(len(payload)) + '}'
    print(gzip.compress(m.encode()))
    sock.sendall(gzip.compress(m.encode()))
    sock.sendall(payload)
    print(gzip.decompress(payload).decode())
    
    