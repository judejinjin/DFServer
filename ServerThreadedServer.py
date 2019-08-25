# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 16:52:07 2019

@author: judejinjin
"""
import pandas as pd
import numpy as np
import socket
import threading
import socketserver
import json
import gzip
import sys

class ThreadedPubSubHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global pubsub
        global pubvalues
        
        cur_thread = threading.current_thread()
        print("ThreadedPubSubHandler thread #" + str(cur_thread))
        while True:
            try:
                cmd = self.request.recv(1024)
                if not cmd:
                    break
                try:
                    print("ThreadedPubSubHandler thread #" + cmd.decode())
                    cmdJson = json.loads(cmd.decode())
                    topic = cmdJson["topic"]
                    if topic not in pubsub:
                        pubsub[topic] = []
                    subscribers = pubsub[topic]
                    subscribers.append(self.request)
                    if topic not in pubvalues:
                        pubvalues[topic] = 0
                    else:
                        pub = '{topic": "' + topic + '", "value": ' + str(pubvalues[topic]) + '}'
                        self.request.sendall(pub.encode())
                except:
                    print("NOK:" + str(sys.exc_info()[0]))
                    continue
            except:
                print("ThreadedPubSubHandler thread #" + str(sys.exc_info()[0]))
                break
        print("ThreadedPubSubHandler thread #" + str(cur_thread) + " is exiting")
        
class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        global root
        
        localScope = {}
        cur_thread = threading.current_thread()
        print("ThreadedTCPRequestHandler thread #" + str(cur_thread))
        while True:
            try:
                cmd = self.request.recv(1024)
                try:
                    cmdStr = gzip.decompress(cmd).decode()
                    if not cmdStr:
                        break
                    cmdJson = json.loads(cmdStr)
                    try:
                        action = cmdJson["cmd"]
                    except:
                        self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                        continue
                    if action == "publish":
                        try:
                            topic = cmdJson["topic"]
                            value = cmdJson["value"]
                            subscribers = pubsub[topic]
                            pubvalues[topic] = value
                            pub = '{topic": "' + topic + '", "value": ' + str(value) + '}'
                            for i in subscribers[:]:
                                try:
                                    i.sendall(pub.encode())
                                except:
                                    print("removing " + str(i))
                                    subscribers.remove(i)
                            continue
                        except:
                            self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                            continue
                    try:
                        table = cmdJson["table"]
                        format = cmdJson["format"]
                        scope = cmdJson["scope"]
                        payloadsize = cmdJson["size"]
                    except:
                        self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                        continue
                    self.request.sendall("OK".encode())
                    if payloadsize > 0:
                        payload = self.request.recv(payloadsize)
                        if len(payload) != payloadsize:
                            self.request.sendall("NOK:payload is corrupt".encode())
                            continue
                        data = gzip.decompress(payload).decode()

                    if action == "insert":
                        if format == "json":
                            df = pd.read_json(data)
                        else:
                            df = df.read_csv(data)                    
                        if scope == "global":
                            context = root
                        else:
                            context = localScope
                        context[table]  = df
                        self.request.sendall("OK".encode())
                    elif action == "execute":
                        try:
                            exec(data)
                            self.request.sendall("OK".encode())
                        except:
                            self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                            continue
                    elif action == "query":
                        try:
                            df = eval(data)
                            if df.empty: 
                                m ='{"size": 0, "status": "OK"}'
                                self.request.sendall(m.encode())
                            else:
                                payload = gzip.compress(df.to_json().encode())
                                m = '{"status": "OK", "size": ' + str(len(payload)) + '}'
                                self.request.sendall(m.encode())
                                self.request.sendall(payload)
                        except:
                            self.request.sendall(('{"size": 0, "status": "NOK:' + str(sys.exc_info()[0]) + '"}').encode())
                            continue
                    elif action == "delete":
                        try:
                            if scope == "global":
                                context = root
                            else:
                                context = localScope
                            df = context.pop(table)    
                            self.request.sendall("OK".encode())
                        except:
                            self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                            continue
                    else:
                        pass
                except:
                    self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
            except:
                print("ThreadedTCPRequestHandler thread #" + str(sys.exc_info()[0]))
                break
        print("ThreadedTCPRequestHandler thread #" + str(cur_thread) + " is exiting")
              
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
        
if __name__ == "__main__":
    root = {}
    pubsub = {}
    pubvalues = {}
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 8888
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)    
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print("Server loop running in thread:", server_thread.name)
    
    HOST2, PORT2 = "127.0.0.1", 8889
    server2 = ThreadedTCPServer((HOST2, PORT2), ThreadedPubSubHandler)    
    server_thread2 = threading.Thread(target=server2.serve_forever)
    server_thread2.daemon = True
    server_thread2.start()
    print("Server loop running in thread:", server_thread2.name)