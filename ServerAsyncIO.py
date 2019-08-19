# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 13:39:59 2019

@author: judejinjin
"""
import pandas as pd
import numpy as np
import asyncio
import threading

async def process(reader, writer):
    addr = writer.get_extra_info('peername')
    while True:
        data = await reader.read(100)  # Max number of bytes to read
        if not data:
            break
        header = data.decode()
        print("Received %r from %r" % (header, addr))
        size = header[2:]
        print("reading: %d" % size)
        data = await reader.read(size)
        if not data:
            break
        content = data.decode()
        
    print("closing socket %r from %r" % (message, addr))
    writer.close()

def main():
    global mainEventLoop
    coro = asyncio.start_server(process, '127.0.0.1', 8888, loop=mainEventLoop)
    server = mainEventLoop.run_until_complete(coro)
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    mainEventLoop.run_forever()
    # Close the server
    server.close()
    mainEventLoop.run_until_complete(server.wait_closed())
    mainEventLoop.close()
    
if __name__ == "__main__":
    mainEventLoop = asyncio.get_event_loop()
    server = threading.Thread(target=main, daemon=True)
    server.start()