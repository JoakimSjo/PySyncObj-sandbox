from collections import defaultdict
from flask import Flask, request
from pysyncobj import SyncObj, replicated, SyncObjConf
from pysyncobj.batteries import ReplCounter
import json
import os
import socket
import sys
from threading import Thread
from queue import Queue
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

web = Queue()
worm = Queue()


def createNode(hostname, nodes=[]):
    counter = ReplCounter()

    conf = SyncObjConf(dynamicMembershipChange=True)
    sync = SyncObj(MY_ADDRESS, nodes, consumers=[counter], conf=conf)
    sync.waitBinded()
    return sync, counter


app = Flask(__name__)
app.secret_key = "notsosecret"
node = None


def worm_th(web_in, worm_out):
    node = None
    counter = None
    nodes = []
    while True:
        # print("waiting on message",  file=sys.stderr)
        print("waiting on message", flush=True)
        # Wait on command
        msg = worm_out.get()
        # print("Got message",  file=sys.stderr)
        print("Got message", flush=True)
        # print(msg, file=sys.stderr)
        print(msg, flush=True)
        command = msg["command"]
        data = msg["data"]
        # print(command,  file=sys.stderr)
        print(command, flush=True)
        try:
            if command == "join":
                node.addNodeToCluster(data)
                others = nodes.copy()
                nodes.append(data)
                web_in.put(others)
            elif command == "start":
                node, counter = createNode(MY_ADDRESS, data)
                nodes = data + [MY_ADDRESS]
                web_in.put(nodes)
            elif command == "nodes":
                web_in.put(nodes)
            elif command == "status":
                web_in.put({"cluster": node.getStatus(),
                            "counter": counter.get()})
            elif command == "leave":
                web_in.put("leaving")
            elif command == "inc":
                web_in.put(counter.inc(sync=True))

        except Exception as e:
            print(e)
        # sleep(2)


t1 = Thread(target=worm_th, args=(web, worm))
t1.start()


@app.route("/start", methods=["POST"])
def start():
    # print("start",  file=sys.stderr)
    print("start", flush=True)
    msg = {"command": "start", "data": []}
    if MY_NODE_NUM == 0:
        worm.put(msg)
        data = web.get()
    else:
        data = request.get_json()
        node_api = data["node_api"]

        response = requests.post("http://" + node_api +
                                 '/join', json={"url": MY_ADDRESS})

        print(response, flush=True)
        others = response.json()
        msg["data"] = others
        worm.put(msg)
        data = web.get()

    return json.dumps(data)


@app.route("/join", methods=["POST"])
def join():
    msg = {"command": "join", "data": []}

    data = request.get_json()
    print(data, flush=True)
    msg["data"] = data["url"]

    worm.put(msg)
    nodes = web.get()

    # cache.modified = True
    return json.dumps(nodes)


@app.route("/inc")
def inc():
    msg = {"command": "inc", "data": []}
    worm.put(msg)
    data = web.get()
    return json.dumps(data)


@app.route("/status")
def status():
    msg = {"command": "status", "data": []}
    worm.put(msg)
    data = web.get()
    # global node
    return json.dumps(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port="500" + str(MY_NODE_NUM))
