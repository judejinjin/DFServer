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
        cur_thread = threading.current_thread()
        
        print("ThreadedTCPRequestHandler thread #" + str(cur_thread))
        while True:
            try:
                cmd = self.request.recv(1024)
                try:
                    print(cmd)
                    cmdStr = gzip.decompress(cmd).decode()
                    print(cmdStr)
                    if not cmdStr:
                        break
                    cmdJson = json.loads(cmdStr)
                    payloadsize = cmdJson["size"]
                    table = cmdJson["table"]
                    
                    data = gzip.decompress(self.request.recv(payloadsize)).decode()
                    print(data)
                    #data = json.loads(data)
                    df = pd.read_json(data)
                    print(df.to_json())
                    root[table]  = df
                except:
                    print("Unexpected error:" + str(sys.exc_info()[0]))
            except:
                print("Unexpected error:" + str(sys.exc_info()[0]))
                break

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
