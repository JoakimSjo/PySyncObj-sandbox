from collections import defaultdict
from flask import Flask, request
from pysyncobj import SyncObj, replicated, SyncObjConf
from pysyncobj.batteries import ReplCounter
import json
import os
import socket

RAFT_PORT = "4321"
MY_NODE_NUM = int(os.environ["MY_NODE_NUM"])
TOTAL_NODES = int(os.environ["TOTAL_NODES"])
ADDRESSES = ["node" + str(n) + ":" + RAFT_PORT for n in range(TOTAL_NODES)]
MY_ADDRESS = ADDRESSES[MY_NODE_NUM]
PEER_ADDRESSES = [ADDRESSES[i] for i in range(TOTAL_NODES) if i != MY_NODE_NUM]
print(PEER_ADDRESSES)
counter = ReplCounter()


app = Flask(__name__)
sync = None


@app.route("/inc")
def inc():
    return json.dumps(counter.inc(sync=True))


@app.route("/status")
def status():
    global sync
    if sync is None:
        return "Not started"

    return json.dumps(sync.getStatus())


@app.route("/start", methods=["POST"])
def start():
    global sync
    app.logger.info("starting")
    data = request.get_json()
    peers = [] if "peers" not in data else data["peers"]
    app.logger.info(peers)
    conf = SyncObjConf(dynamicMembershipChange=True)
    sync = SyncObj(MY_ADDRESS, peers, consumers=[counter], conf=conf)
    sync.waitBinded()
    app.logger.info("started")
    return "Started"


@app.route("/join", methods=["POST"])
def join():
    global sync
    if sync is None:
        return "Not started"

    data = request.get_json()
    join = data["url"]

    app.logger.info(data["url"])
    sync._addNodeToCluster(join, lambda res, error: app.logger.info(res))
    return "Ok"


if __name__ == "__main__":
    print(MY_ADDRESS)
    app.run(host="0.0.0.0",  debug=True, port="500" + str(MY_NODE_NUM))
