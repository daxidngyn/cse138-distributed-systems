from flask import Flask, jsonify, request, Response
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import time
import requests
import logging
import json
from types import SimpleNamespace
import uuid
import traceback
import random

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
VIEW = os.environ.get('VIEW')
# for debugging. e.g., LOG_LEVEL='DEBUG' for debug
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'WARNING').upper()
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=LOG_LEVEL)

# managed by View, used by Kv_store
local_causal_metadata = []
md_lock = threading.Lock() # to sync changes to local_causal_metadata

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

class VClock:
    def __init__(self, id=None, clock_init_val=0):
        self.id = id
        self.clock = clock_init_val

    def tick(self):
        self.clock += 1

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def from_json(s):
        s = s.replace("\'", "\"")
        o = json.loads(s, object_hook=lambda d: SimpleNamespace(**d))
        return VClock(o.id, o.clock)
    
    def __str__(self) -> str:
        return self.id + "," + str(self.clock)
    def __repr__(self) -> str:
        return self.__str__()
    
class CausalMetadata:
    ''' data structure to contain causal metadata '''
    def __init__(self, action, key, value, vclocks=None, sender_id="client"):
        self.sender_id = sender_id
        self.action = action
        self.key = key
        self.value = value
        # array of VClock
        self.vector_clocks = vclocks

    def get_vclock(self, vcid):
        if self.vector_clock:
            for vc in self.vector_clocks:
                if vc.id == vcid:
                    return vc
        return None
        
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def from_json(s):
        s = s.replace("\'", "\"")
        o = json.loads(s, object_hook=lambda d: SimpleNamespace(**d))
        return CausalMetadata(o.action, o.key, o.value, o.vector_clocks, o.sender_id)

    def from_dict(d):
        ret = CausalMetadata(None, None, None)
        if "action" in d:
            ret.action = d["action"]
        if "key" in d:
            ret.action = d["key"]
        if "sender_id" in d:
            ret.action = d["sender_id"]
        if "value" in d:
            ret.action = d["value"]
        if "vector_clocks" in d:
            arr = d["vector_clocks"]
            clocks = []
            for i in arr:
                clock = i["clock"]
                id = i["id"]
                clocks.append(VClock(id, clock))
            ret.vector_clocks = clocks
        return ret

    def __repr__(self) -> str:
        return self.to_json()
     
class CRequest:
    ''' data structure to capture a request to be processed later '''
    def __init__(self, sender_id, action, key, value, causal_md: CausalMetadata):
        # sender_id is to distinguish a req by a peer from a req by clients
        self.id = str(uuid.uuid4())
        self.sender_id = sender_id
        self.action = action
        self.key = key
        self.value = value
        self.causal_metadata = causal_md

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def __repr__(self) -> str:
        return self.to_json()

class RepeatTask(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class View:
    def __init__(self):
        # control whether instance is active/live
        self.addr = SOCKET_ADDRESS.strip()
        self.addresses = [i.strip() for i in VIEW.split(',') if len(i) > 0 and not i.isspace()]
        self.non_reponsive_peers = {} # map addr -> number of tries
        '''
        for addr in self.addresses:
            if addr != self.addr:
                t = threading.Thread(self._notify_peer, addr)
                t.daemon = True  # thread will die when app shuts down
                t.start()
        '''
        t2 = threading.Thread(target=self._check_peers)
        t2.daemon = True  # thread will die when app shuts down
        t2.start()

    def _notify_peer(self, addr):
        ''' when brought online, notify peers about its presence '''
        count = 0
        while count < 4:
            status_code = self._send(addr, "PUT", self.addr)
            if status_code == 200 or status_code == 201:
                count = 4
            time.sleep(1)
            count += 1

        
    def _check_peers(self):
        while True:
            time.sleep(2)
            to_delete = []
            to_restore = []
            for addr in self.addresses:
                if addr != self.addr:
                    #logging.info(f"Pinging {addr}")
                    resp_status = self._ping(addr)
                    if resp_status != 201:
                        if addr not in self.non_reponsive_peers:
                            self.non_reponsive_peers[addr] = 1
                        else:
                            self.non_reponsive_peers[addr] += 1
                        if self.non_reponsive_peers[addr] > 3:
                            to_delete.append(addr)
                    else:
                        to_restore.append(addr)
            for addr in to_restore:
                if addr in self.non_reponsive_peers:
                    del self.non_reponsive_peers[addr]
            for addr in to_delete:
                #logging.info(f"Removed dead instance {addr}")
                self.addresses.remove(addr)
                del self.non_reponsive_peers[addr]
                    
            for remaining_addr in self.addresses:
                if remaining_addr != self.addr:
                    for addr in to_delete:
                        logging.info(f"Inform {remaining_addr} to remove {addr}")
                        self._send(remaining_addr, "DELETE", addr)

    def _ping(self, addr: str):
        ''' ping addr with /helo URL to see if it's still alive '''
        try:
            session = requests.Session()
            url = "http://" + addr + "/helo"
            data = {'socket-address': self.addr}
            response = session.put(url, json=data)
            #response = requests.get(url)
            return response.status_code
        except Exception as e:
            logging.warning(f"{addr} did not respond")
            #logging.warning(e)
        return -1

    def greet(self, socket_address):
        if socket_address not in self.addresses:
            self.addresses.append(socket_address)
        if socket_address in self.non_reponsive_peers:
            del self.non_reponsive_peers[socket_address]
        return jsonify({"result": "already present"}), 201

    def put(self, socket_address, sender_id):
        # if instance is dormant and request comes from client, ignore request
        if sender_id is None and self.addr is None:
            return jsonify({"error": "Service unavailable"}), 503
        
        if socket_address in  self.addresses: 
            return jsonify({"result": "already present"}), 201

        self.addresses.append(socket_address)
        # only inform peers if request comes from client, not from a peer
        if sender_id is None:
            self._broadcast("PUT", socket_address)
        elif self.addr is None and SOCKET_ADDRESS.strip() == socket_address:
            # bring it back up live
            self.addr = socket_address

        #resp = Response(response=jsonify({"result": "added"}), status=200)
        #return allow_cross_origin(resp)
        return jsonify({"result": "added"}), 200
    
    def get(self, sender_id):
        # if instance is dormant and request comes from client, ignore request
        if sender_id is None and self.addr is None:
            return jsonify({"error": "Service unavailable"}), 503

        return jsonify({"view":  self.addresses}), 200
    
    def delete(self, socket_address, sender_id):
        # if instance is dormant and request comes from client, ignore request
        if sender_id is None and self.addr is None:
            return jsonify({"error": "Service unavailable"}), 503

        if socket_address not in  self.addresses:
            return jsonify({"error": "View has no such replica"}), 404

        # only inform peers if request comes from client, not from a peer
        if sender_id is None:
            self._broadcast("DELETE", socket_address)
        self.addresses.remove(socket_address)

        if self.addr == socket_address:
            logging.info(f"{self.addr} taking myself offline. self.addr = {self.addr}")
            self.addr = None
        return jsonify({"result": "deleted"}), 200

    def _broadcast(self, action, socket_address):
        # let's do this synchronously
        peers = list(self.addresses)
        peers.remove(SOCKET_ADDRESS)
        threads = []
        for peer_id in peers:
            print(f"sending req to {peer_id}")
            t = threading.Thread(target=self._send, group=None, name="Thread-" + peer_id, args=[peer_id, action, socket_address])
            t.start()
            threads.append(t)
        # to do async, don't join
        #for t in threads:
        #    t.join()

    def _send(self, receiver_id: str, action, socket_address):
        try:
            headers = {'Content-type': 'application/json'}
            url = "http://" + receiver_id + "/view"
            logging.debug(f"sending to {url}")
            # include the sender-id so that the peer will not re-broadcast
            data = {"socket-address": socket_address, "sender-id": self.addr}
            logging.debug(data)
            if action == "PUT":
                response = requests.put(url, json=data, headers=headers)
            elif action == "DELETE":
                response = requests.delete(url, json=data, headers=headers)
            logging.debug(f"{response.url}'s response: {response.status_code}, {response.text}")
            return response.status_code
        except:
            return -1

class Kv_store:
    def __init__(self, view):
        self.kv_store = {}       
        # URL address, also as ID, also to control whether this instance
        # is active
        self.addr = SOCKET_ADDRESS.strip()
        # initialize all clocks
        self.vclocks = {} 
        for addr in view.addresses: # peers' clocks
            self.vclocks[addr] = VClock(addr)

        # thread pool to broadcast to peers
        self.thread_pool = ThreadPoolExecutor(min(1, len(self.vclocks)))
        # delayed queue for CRequest
        self.delayed_queue = [] # list of CRequest instances

    def on_peer_change(self, addresses):
        for addr in addresses:
            if addr not in self.vclocks: # add a new one
                self.vclocks[addr] = VClock(addr)
                # (re)activate this instance
                if self.addr is None:
                    self.addr = SOCKET_ADDRESS.strip()
        to_drop = []
        for addr in self.vclocks:
            if addr not in addresses: # drop one
                to_drop.append(addr)
                # if this instance is dropped, inactivate it
                if addr == self.addr:
                    logging.info(f"{self.addr} taking myself off line")
                    self.addr = None
        for i in to_drop:
            logging.debug(f"{self.addr} drops clock {i}")
            del self.vclocks[i]

    
    def _update_clocks(self, clock_array=None):
        ''' update self.vclocks
        '''
        if clock_array: 
            for clock in clock_array:
                vclock = self.vclocks[clock.id]
                vclock.clock = max(vclock.clock, clock.clock)
        else: # just tick our own clock
            self.vclocks[self.addr].tick()
        
    def put(self, key, value, req_causal_metadata, sender_id):
        return self.write("PUT", key, value, req_causal_metadata, sender_id)

    def write(self, action, key, value, req_causal_metadata, sender_id):
        if not self.addr:
            return jsonify({"error": "Service unavailable"}), 503
        
        if sender_id is None:
            sender_id = "client"
        
        resp_code = 201
        resp_data = {}
        req_md = None # the causal metadata included in the request
        if req_causal_metadata:
            req_md = CausalMetadata.from_dict(req_causal_metadata)
            
        if req_md:
            # check dependencies
            # The causal metadata indicates that PUT(y,2) causally depends on PUT(x,1)
            if not self._causally_deliverable(sender_id, self.vclocks, req_md):
                creq = CRequest(req_md.sender_id, action, key, value, req_md)
                self.delayed_queue.append(creq)
                return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503

        current_req = CRequest(sender_id, action, key, value, req_md)
        resp_data, resp_code = self._process_request(current_req)

        # now we must see if there is anything in the delay queue that
        # needs to be processed before this
        reqs_from_queue = self._messages_to_process(current_req)
        for creq in reqs_from_queue:
            self._process_request(creq)

        # CausalMetadat can't be converted to JSON by jsonify(), so we have to do
        # convert to json manually
        response = Response(
            response=json.dumps(resp_data, default=lambda o: o.__dict__),
            status=resp_code,
            mimetype='application/json'
            )
        return response

    def _process_request(self, creq: CRequest):
        logging.debug(f"CRequest: {creq}")
        action = creq.action
        key = creq.key
        resp_data = {}
        resp_code = -1
        if action == "PUT":
            if creq.sender_id == "client":
                self._update_clocks()
                causal_md = CausalMetadata(action, key, creq.value, list(self.vclocks.values()), self.addr)
                self._broadcast(action, key, creq.value, causal_md)
                if key in self.kv_store:
                    resp_data["result"] = "replaced"
                    resp_code = 200
                else:
                    resp_code = 201
                    resp_data["result"] = "created"
                # set the ID to client before returning it to client
                causal_md.sender_id = "client"
                resp_data["causal-metadata"] = causal_md
            else: # message from a peer
                logging.debug("sender is peer")
                self._update_clocks(creq.causal_metadata.vector_clocks)
            self.kv_store[key] = creq.value
        elif action == "DELETE":
            if creq.sender_id == "client":
                self._update_clocks()
                causal_md = CausalMetadata(action, key, creq.value,  list(self.vclocks.values()), self.addr)
                self._broadcast(action, key, creq.value, causal_md)
                if key not in self.kv_store:
                    resp_data["error"] = "Key does not exist"
                    resp_code = 404
                else:
                    del self.kv_store[key]
                    resp_code = 200
                    resp_data["result"] = "deleted"
                    # set the ID to client before returning it to client
                    causal_md.sender_id = "client"
                    resp_data["causal-metadata"] = causal_md
            else: # message from a peer
                if key in self.kv_store:
                    del self.kv_store[key]
                self._update_clocks(creq.causal_metadata.vector_clocks)

        return resp_data, resp_code

    def _causally_deliverable(self, sender_id, current_clocks, req_md: CausalMetadata):
        ''' Causal deliverability depends on two factors: action dependency and clock 
            dependency.
        '''
        return self._action_deliverable(req_md) and \
            self._clock_deliverable(sender_id, current_clocks, req_md.vector_clocks)


    def _action_deliverable(self, req_md: CausalMetadata):
        ''' Action dependency is specified by the causal action included in
            the causal metadata.

            e.g., if the causal action is PUT(x,1), the current action is only
            deliverable if 1 is already stored for x in the KVS.
        '''
        if not req_md:
            return True
        # first, check the causal action
        causal_action = req_md.action
        if causal_action == "PUT":
            try:
                if req_md.value  != self.kv_store[req_md.key]:
                    return False
            except KeyError:
                return False
        elif causal_action == "DELETE":
            if req_md.key in self.kv_store:
                return False

        return True

    def _clock_deliverable(self, sender_id, current_clocks, incoming_clocks):
        ''' Clock dependency deals with the ordering of messages between replicas.

            e.g., client_1 hits Alice with PUT(x, 1) then client_2 hits Alice with PUT(x, 2)
            Alice broadcasts to Bob CM1={PUT(x,1), [1,0,0]} for the first message and
            CM2={PUT(x,2), [2,0,0]} for the second message. Suppose the second message 
            arrives at Bob first. Bob cannot process it since it's out of order.
         
            p: the receiving process, m the incoming msg.

            Params:
            - sender_id : the ID of the sender, k
            - current_clocks: current process clocks, VC(p), in map: id -> VC(p)[id]
            - incoming_clocks: the array of clocks of all nodes, VC(m)
            # VC(m)[k] = VC(p)[k] + 1, k is sender
            # VC(m)[k] <= VC(p)[k], other k
            # VC(m): VC of the incoming msg
            # VC(p): VC on the process
        '''
        # only check for messages from a peer, not client
        if sender_id == "client":
            return True
        for clock in incoming_clocks:
            current_clock = current_clocks[clock.id]
            if clock.id == sender_id: # if clock is sender's clock
                if clock.clock != (current_clock.clock + 1):
                    return False
            else:
                if clock.clock > current_clock.clock:
                    return False
        return True

    def _messages_to_process(self, req: CRequest):
        '''
            Look through the delayed queue for all messages (CRequest) that
            were put there due to causal dependencies for sender_id.

            Parameters:
            - req: CRequest object

            Return:
            - list of CRequest objects
        '''
        to_process = [req]
        last_req: CRequest = None
        while True:
            if last_req and last_req.id == to_process[-1].id:
                break
            last_req = to_process[-1]
            current_clock_map = {}
            req_md = last_req.causal_metadata
            if req_md:
                for c in last_req.causal_metadata.vector_clocks:
                    current_clock_map[c.id] = c
                for req in self.delayed_queue:
                    if self._causally_deliverable(last_req.sender_id, current_clock_map, req.causal_md):
                        to_process.append(req)
                        self.delayed_queue.remove(req)
                        break
        # remove the first one, since it was processed separately
        to_process.remove(req)
        return to_process
            
            
    def _broadcast(self, action, key, value, causal_md: CausalMetadata):
        # fire-and-forget to peers
        for clock_id in self.vclocks:
            if clock_id != self.addr:
                self.thread_pool.submit(self._send_to_peer, clock_id, action, key, value, causal_md)

    def _send_to_peer(self, id, action, key, value, causal_md: CausalMetadata):
        # fire and forget; we assume receivers know how to deal with out-of-order messages.
        # Plus, we don't have to worry about any peer leaving the cluster
        try:
            headers = {'Content-type': 'application/json'}
            url = "http://" + id + "/kvs/" + key
            logging.debug(f"sending to {url}")
            # ensure the sender-id in causal metadat to be from this instance
            causal_md.sender_id = id
            data = {"value": value, "causal-metadata": causal_md, "sender-id": self.addr}
            data = json.dumps(data, default=lambda o: o.__dict__)
            data = data.replace("\'", "\"")
            logging.debug(f"data = {data}")
            response = None
            if action == "PUT":
                logging.debug(f"sending PUT to {url}")
                response = requests.put(url, data=data, headers=headers)
            elif action == "DELETE":
                response = requests.delete(url, data=data, headers=headers)
            logging.debug(f"{id} response code: {response.status_code}")
        except:
            logging.warning(traceback.format_exc())

    def get(self, key, req_causal_metadata, sender_id):
        if not self.addr:
            return jsonify({"error": "Service unavailable"}), 503
        
        if sender_id is None:
            sender_id = "client"

        # still must check for dependencies if applicable            
        if key in self.kv_store:
            resp_data = {"result": "found", "value": self.kv_store[key]}
            if req_causal_metadata:
                resp_data['causal-metadata'] = CausalMetadata.from_dict(req_causal_metadata)
            response = Response(
                response=json.dumps(resp_data, default=lambda o: o.__dict__),
                status=200,
                mimetype='application/json'
            )
            #return jsonify({"result": "found", "value": self.kv_store[key]}), 200
            return response
        
        return jsonify({"error": "Key does not exist"}), 404
    
    def delete(self, key, req_causal_metadata, sender_id):
        return self.write("DELETE", key, None, req_causal_metadata, sender_id)
                 
        
view = View()
kv_store = Kv_store(view)

@app.route('/helo', methods=['PUT'])
def hello():
    try:
        view.greet(request.json['socket-address'])
        return jsonify({"message": "ok"}), 200
    except:
        logging.debug(traceback.format_exc())
        return jsonify({"error": "PUT request does not specify a value"}), 400

# This endpoint adds a new replica to the view
@app.route('/view', methods=['PUT', 'GET', 'DELETE'])
def view_operations():
    try:
        sender_id = request.json['sender-id']
    except:
        sender_id = None

    if request.method == 'PUT':
        try:
            socket_address = request.json['socket-address']
            print (f"socket_address = {socket_address}")
            with md_lock:
                ret = view.put(socket_address, sender_id)
                print(ret)
                kv_store.on_peer_change(view.addresses)
            return ret
        except KeyError as e:
            return jsonify({"error": "Must specify socket-address in body"}), 400

    if request.method == 'GET':
        try:
            j, c = view.get(sender_id)
            return j, c
        except Exception as e:
            logging.debug(traceback.format_exc())

    if request.method == 'DELETE':
        try:
            socket_address = request.json['socket-address']            
            with md_lock:
                ret = view.delete(socket_address, sender_id)
                print(ret)
                kv_store.on_peer_change(view.addresses)
            return ret
        except Exception as e:
            logging.debug(traceback.format_exc())
            return jsonify({"error": "Must specify socket-address in body"}), 400

@app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
def kvs_operations(key):
    causal_metadata = None
    try:
        causal_metadata = request.json['causal-metadata']
    except:
        logging.debug(traceback.format_exc())
        causal_metadata = None

    try:
        sender_id = request.json['sender-id']
    except:
        logging.debug(traceback.format_exc())
        sender_id = None

    if request.method == 'GET':
        try:
            return kv_store.get(key, causal_metadata, sender_id)
        except:
            logging.debug(traceback.format_exc())

    if request.method == 'PUT':
        # check for errors in req
        if len(key) > 50:
            return jsonify({"error": "Key is too long"}), 400

        value = None
        try:
            value = request.json['value']
        except:
            logging.debug(traceback.format_exc())
            return jsonify({"error": "PUT request does not specify a value"}), 400
        logging.info(f"receive from {sender_id} PUT({key}, {value}) with md {causal_metadata}")
        # no errors, so proceed
        return kv_store.put(key, value, causal_metadata, sender_id)

    if request.method == 'DELETE':
        return kv_store.delete(key, causal_metadata, sender_id)


if __name__ == '__main__':
    a = SOCKET_ADDRESS.split(":")
    #app.run(host=a[0], port=a[1], debug=True)
    app.run(host='0.0.0.0', port=a[1], debug=True)