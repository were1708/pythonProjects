import globals
from flask import Flask, request, jsonify, Blueprint
import requests
import threading
import os
import asyncio
from broadcast import broadcast_shard


def send_request():
    # print("hello")
    while len(globals.current_view) > 0:
        os.sleep(5)
        url = f'http://{globals.address}/kvs/data'
        response = requests.get(url)

def sync():
    # print("FUA")
    globals.syncThread = threading.Thread(target=send_request())

def sync_2():
    try:
        asyncio.run(broadcast_shard(globals.shard_view[globals.shard_member],'PUT', '/kvs/internal/sync', globals.local_data, globals.local_clocks, node_id=globals.last_write))
    except:
        pass
    
        