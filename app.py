from collections import defaultdict
from flask import Flask, request
from pysyncobj import SyncObj, replicated, SyncObjConf
from pysyncobj.batteries import ReplCounter
import json
import os
import socket
import threading
from time import sleep
import requests
# Dynamic join procedure
# 1. Call addNodeToCluster on one of the exisiting nodes in the cluster.
# 2. Wait for callback succeding before creating the new node, this is not nessecary when
# going from 1 to 2 nodes.
# 3. Create the new node with the correct list of other nodes in the cluster.

RAFT_PORT = "4321"
MY_NODE_NUM = int(os.environ["MY_NODE_NUM"])
TOTAL_NODES = int(os.environ["TOTAL_NODES"])
ADDRESSES = ["node" + str(n) + ":" + RAFT_PORT for n in range(TOTAL_NODES)]
MY_ADDRESS = ADDRESSES[MY_NODE_NUM]
PEER_ADDRESSES = [ADDRESSES[i] for i in range(TOTAL_NODES) if i != MY_NODE_NUM]
nodes = []


def createNode(hostname, nodes=[]):
    counter = ReplCounter()
    conf = SyncObjConf(dynamicMembershipChange=True)
    sync = SyncObj(MY_ADDRESS, nodes, consumers=[counter], conf=conf)
    sync.waitBinded()
    return sync


app = Flask(__name__)
app.secret_key = "notsosecret"
node = None

# Join after different times
sleep(10 * MY_NODE_NUM)
if MY_NODE_NUM == 0:
    node = createNode(ADDRESSES[0])
    node.addNodeToCluster(ADDRESSES[1], lambda a, b: print(f"a: {a} ,b: {b}"))

if MY_NODE_NUM == 1:
    # 1. /join -> "node1:4321"
    # 2. addNodeToCluster "node1:4321"
    # 3. -> ["node0:4321"]
    # 4. SyncObj("node1:4321", ["node0:4321", "node2:4321"])
    # /join => [node0:4321, node1:4321]
    node = createNode(ADDRESSES[1], nodes=[ADDRESSES[0]])
    node.addNodeToCluster(ADDRESSES[2])
elif MY_NODE_NUM == 2:
    node = createNode(ADDRESSES[2], nodes=ADDRESSES[:2])
#sync._addNodeToCluster(ADDRESSES[0], lambda a, b: print(f"a: {a} ,b: {b}"))
elif MY_NODE_NUM != 0:
    node = createNode(ADDRESSES[MY_NODE_NUM])
#cache = {}


@app.route("/start", methods=["POST"])
def start():
    if MY_NODE_NUM == 0:
        cache['node'] = createNode(MY_ADDRESS, [])
        cache['nodes'] = [MY_ADDRESS]
    else:
        data = request.get_json()
        node_api = data["node_api"]
        print(requests.get(f"http://{node_api}/status".strip()))
        response = requests.post("http://" + node_api +
                                 '/join', json={"url": MY_ADDRESS})
        print(response)
        others = response.json()
        cache['nodes'] = others + [MY_ADDRESS]
        print(others)
        cache['node'] = createNode(MY_ADDRESS, others)
    # sleep(4)
    #cache.modified = True
    return json.dumps(cache['nodes'])


@app.route("/join", methods=["POST"])
def join():
    #global node, nodes
    data = request.get_json()
    joiner = data["url"]
    others = cache['nodes'].copy()
    cache['nodes'].append(joiner)
    cache['node'].addNodeToCluster(joiner)
    # sleep(10)

    #cache.modified = True
    return json.dumps(others)


@app.route("/inc")
def inc():
    return json.dumps(counter.inc(sync=True))


@app.route("/status")
def status():
    #global node
    return json.dumps(node.getStatus())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port="500" + str(MY_NODE_NUM))
