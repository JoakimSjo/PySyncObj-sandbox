from collections import defaultdict
from flask import Flask, request
from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplCounter
import json
import os
import socket

RAFT_PORT = '4321'
MY_NODE_NUM = int(os.environ['MY_NODE_NUM'])
TOTAL_NODES = int(os.environ['TOTAL_NODES'])
ADDRESSES = ['node' + str(n) + ':' + RAFT_PORT for n in range(TOTAL_NODES)]
MY_ADDRESS = ADDRESSES[MY_NODE_NUM]
PEER_ADDRESSES = [ADDRESSES[i] for i in range(TOTAL_NODES) if i != MY_NODE_NUM]

counter = ReplCounter()
sync = SyncObj(MY_ADDRESS, PEER_ADDRESSES, consumers=[counter])
sync.waitBinded()

app = Flask(__name__)

@app.route('/inc')
def inc():
    return json.dumps(counter.inc(sync=True))

@app.route('/status')
def status():
    return json.dumps(sync.getStatus())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='500' + str(MY_NODE_NUM))
