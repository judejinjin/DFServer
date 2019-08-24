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
                    elif action == "execute":
                        try:
                            exec(data)
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
                        except:
                            self.request.sendall(("NOK:" + str(sys.exc_info()[0])).encode())
                            continue
                    else:
                        pass
                    self.request.sendall("OK".encode())
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
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 8888

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    print("Server loop running in thread:", server_thread.name)
