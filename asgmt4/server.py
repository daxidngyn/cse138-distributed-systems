import os
import requests
import socket
from flask import Flask, request, jsonify
import json
from paxos import Proposer, Acceptor
import math
import random
import hashlib
import logging
import sys
import time
from collections import OrderedDict

DEBUG=os.environ.get('DEBUG')
logging.getLogger().addHandler(logging.FileHandler('app.log'))
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
if DEBUG == "1":
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
VIEW = os.environ.get('VIEW')

SHARD_COUNT = os.environ.get('SHARD_COUNT') # must decide how to organize view into shards
SHARD_NAMES = ['alligator','buffalo','cat',"dog",'elephant','fox','goat','horse','iguana','jaguar']

class Server:
    def __init__(self, name):
        self.app = Flask(name)
        with self.app.app_context():
            self.view = self.View(self.app)
            self.kv_store = self.KV_Store(self.app, self.view)
            self.shard = self.Shard(self.view, self.kv_store, self._notify)

        print(f"Server running on {SOCKET_ADDRESS}")

        @self.app.route('/view', methods=['PUT', 'GET', 'DELETE'])
        def view_api():
            if request.method == 'PUT':
                try:
                    socket_addr = request.json['socket_address']
                    res = self.view.put(socket_addr)
                    self.kv_store.update_view(self.view)
                    return res
                except KeyError:
                    return jsonify({"error": "[view_api] PUT request does not specify a value"}), 400
            if request.method == 'GET':
                return self.view.get()
            if request.method == 'DELETE':
                try:
                    socket_addr = request.json['socket_address']          
                    res = self.view.delete(socket_addr)
                    self.kv_store.update_view(self.view)
                    return res
                except KeyError:
                    return jsonify({"error": "DELETE request does not specify a value"}), 400
                
        @self.app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
        def kvs_api(key):
            if request.method == 'PUT':
                try:
                    # self.app.logger.info(f"Received PUT request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    logging.info(f"[kvs_api] incoming key = {key}, value = {value}")
                    broadcast = None
                    if 'broadcast' in request.json:
                        broadcast = request.json['broadcast']
                        
                    res = self.kv_store.put(key, value, request.json, broadcast)
                    data, status_code = res["data"], res["status_code"]
                    logging.info(f"Successful PUT {key} {status_code}:\n\t{data}")

                    return data, status_code
                except KeyError as e:
                    logging.info(f"error: {e}")
                    return jsonify({"error": "[kvs_api] PUT request does not specify a value"}), 400
            if request.method == 'GET':
                try:
                    if 'causal-metadata' in request.json:
                        self.app.logger.info(f"Received GET {key} request on socket {SOCKET_ADDRESS}: {request.json}")
                        return self.kv_store.get(key, request.json['causal-metadata'])
                    self.app.logger.info(f"GET {key} no causal md request on socket {SOCKET_ADDRESS}")
                    ret = self.kv_store.get(key, None)
                    logging.info("[kvs_api] GET returns {ret}")
                    return ret
                except Exception as e:
                    logging.warning(f"[kvs_api] GET error: {e}")
                    return jsonify({"error" : str(e)}), 506
            if request.method == 'DELETE':
                try:
                    # self.app.logger.info(f"Received DELETE request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    return self.kv_store.put(key, value, request.json, 'broadcast' in request.json)
                except KeyError as e:
                    logging.warning(f"error: {e}")
                    return jsonify({"error": "DELETE request does not specify a value"}), 400
                except Exception as e1:
                    logging.error(f"Something BAD went wrong:\n\t{e1}")

        @self.app.get('/kvs/fetchAll')
        def kvs_fetch_all():
            return self.kv_store.fetch_all()      

        @self.app.put('/kvs/loadAll')
        def kvs_load_all():
            try:
                return self.kv_store.load_all(request.json["kvs"])      
            except KeyError:
                return jsonify({"error": "PUT /kvs/loadAll must specify 'kvs' in body"}), 404
            
        @self.app.route('/shard/ids')
        def shard_query_all():
            return self.shard.get("ids")
        
        @self.app.get('/shard/node-shard-id')
        def shard_query_one():
            return self.shard.get("id")
        
        @self.app.get('/shard/members/<id>')
        def shard_members(id):
            return self.shard.get("members", id)
        
        @self.app.get('/shard/key-count/<id>')
        def shard_count(id):
            return self.shard.get("keycount", id)
        
        @self.app.put('/shard/add-member/<id>')
        def shard_add_members(id):
            try:
                broadcast = True
                if 'broadcast' in request.json and request.json["broadcast"]:
                    broadcast = request.json["broadcast"]
                logging.info(f"[add-member] PUT req broadcast = {broadcast}, type {type(broadcast)}")
                return self.shard.put(id, request.json["socket-address"], 'broadcast' not in request.json)
            except KeyError:
                return jsonify({"error": "PUT add-member must specify 'socket-address' in body"}), 404

        # node receiving req initiates resharding of entire kv store
        # keys should be redistributed across new shards
        @self.app.put('/shard/reshard')
        def reshard():
            try:
                shard_count = int(request.json["shard-count"])
                if shard_count < 0:
                    return jsonify({"error": "'shard-count' in body must be positive integer"}), 404
                return self.shard.reshard(shard_count)
            except KeyError:
                return jsonify({"error": "must specify 'shard-count' in body"}), 404
            except ValueError:
                return jsonify({"error": "'shard-count' in body must be positive integer"}), 404

        # This is used among the nodes for sharding
        @self.app.post('/shard-alloc')
        def shard_alloc():
            res = self.shard.on_paxos_msg(request.get_json())
            return jsonify(res)
        

    def _notify(self, type: str, context: dict, results: dict = None ):
        ''' A way to avoid coupling of components 
            Parameters:
            - type: some keyword the notifier uses to signal what action to take
            - context: the input to a function/method to invoke
            - results: to store return values (if applicable)
        '''
        if type == "shard_members":
            self.kv_store.update_shard_info(context, results)
        elif type == "kv_size":
            self.kv_store.size(results, context)

    class Shard:
        def __init__(self, view, kvs, callback=None):
            '''
                Params:
                - view: View
                - callback: should be Server.notify
            '''
            self.kvs = kvs
            self.notify = callback 
            self.view = view
            self.address = SOCKET_ADDRESS

            # new nodes are NOT given SHARD_COUNT, should join the view as usual but
            # not participate in any shard until store recieves /shard/add-member/<id> req
            if SHARD_COUNT:
                self.shard_count = int(SHARD_COUNT)
            else:
                self.shard_count = 1

            self.proposer = Proposer(self.address, self.view)
            self.acceptor = Acceptor(self.address, self.view)
            self.processed_proposals = {}
            # map shard ID to list of nodes in the shard
            # Why not use kvs.shard_members? We don't want to couple
            # this one and KV_Store. Instead, we use the callback
            # as the bridge between the two.
            self.shard_members: dict = {} # shard_id -> [addr]
            
            # decide if it wants to be a Proposer
            self.is_proposer = False
            if self.address:
                ip = self.address.split(':')[0]
                last = int(ip.split('.')[-1])
                denom = math.floor(len(self.view.view)/2)
            #logging.info(f"##### math.floor(len(self.view.view)/2) = {denom}, last digit = {last}")
            # if I'm the last one in view, I'll broadcast
                if self.address == self.view.view[-2]: 
                    self.propose_shard_allocs_quick(self.shard_count)

        def reshard(self, shard_count):
            return self.propose_shard_allocs_quick(shard_count, True)

        def propose_shard_allocs_quick(self, shard_count, do_test_run=False):
            logging.info(f"###### {self.address} runs propose_shard_allocs_quick")
            if do_test_run:
                if self._redistribute_shards(shard_count, self.view.view) is None:
                    return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400

            #if not do_test_run and self.shard_members: # only run 1 time
            #    return

            self.shard_members = self._redistribute_shards(shard_count, self.view.view)
             
            proposal = { "sender_id": self.address, "number": self.proposer.proposal_number}
            value = { "shards": self.shard_members }
            payload = {
                "type": "ACCEPTED",
                "proposal": proposal,
                "accepted_value": value
            }
            all_addresses = self.view.view
            to_send = [ x for x in all_addresses if x != self.address ]
            logging.info(f"###### {self.address} sending {self.shard_members}")
            results = self.acceptor.send_accepted(payload)
            self.proposer.proposal_number += 1
            self.notify("shard_members", self.shard_members)
            #self.kvs.update_shard_info(self.shard_members)

            return jsonify({"result": "resharded"})

        def propose_shard_allocs(self, shard_count):
            logging.info(f"###### {self.address} invokes propose_shard_allocs")
            self.is_proposer = True
            shards = self.shard_members.copy() # don't change yet
            shards = self._redistribute_shards(shard_count, self.view.view)
            checked = self._check_minimum_shard_count(shards)

            if checked == False:
                return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400

            payload = { "shards": shards }
            all_addresses = self.view.view
            to_send = [ x for x in all_addresses if x != self.address ]
            to_avoid = []
            count = 0
            while count < 3:
                to_send = [ x for x in to_send if x not in to_avoid ]
                results = self.proposer.prepare()
                #logging.info(f"Proposer {self.address} gets results: {results}")
                proposal = results[0]
                agreeds = results[1]
                other_proposals = results[2]

                quorum_size = float(len(to_send)) / 2.0
                logging.info(f"{self.address} quorum size = {quorum_size}")

                if len(agreeds) >= quorum_size:
                    results = self.proposer.send_accept(agreeds, proposal, payload)
                    break
                elif len(agreeds) > len(other_proposals):
                    logging.info(f"Feeling lucky. Try again.")
                    for proposal in other_proposals:
                        if proposal["sender_id"] != self.address:
                            to_avoid.append(proposal["sender_id"])
                    count += 1
                else:
                    for proposal in other_proposals:
                        if proposal["sender_id"] != self.address:
                            self.is_proposer = False
                            break
                    break
            return jsonify({"result": "resharded"})

        def _check_minimum_shard_count(self, shards):
            if not shards or len(shards) == 0:
                return False
            for shard_id, members in shards.items():
                if len(members) < 2:
                    return False
            return True
        
        def _redistribute_shards(self, shard_count, view: list):
            ''' Redistribute the shards so that the number of shards == shard_count
                and each shard has at least 2 members.
                
                This algo tries to spread evenly the nodes into shards, maintaining
                as many as possible old members of the shards. This may result in
                a rebalance where some nodes may be transferred from a shard with
                many nodes to a shard with few nodes.
                
                If shard_count > len(shards), more shards will be added.

                Returns:
                - a dict of shard_id to [node_addr]
            '''
            total = len(view)
            if total < shard_count * 2:
                return None
            nodes_per_shard =  int(total / shard_count)
            extra = total % shard_count
            shards = {}
            all_nodes = view.copy()
            all_nodes = sorted(all_nodes)
            
            #print(f"nodes_per_shard = {nodes_per_shard}")
            for i in range(shard_count):
                shard_id = SHARD_NAMES[i]
                shard_members = []
                for _ in range(nodes_per_shard):
                    if all_nodes:
                        shard_members.append(all_nodes.pop(0))
                if extra:
                    shard_members.append(all_nodes.pop(0))
                    extra -= 1
                shards[shard_id] = shard_members 
            logging.info(f"##### [_redistribute_shards] shars = {shards}")
            return shards

        def on_paxos_msg(self, message: str):
            ''' Parameters:
                - message: string representation of a JSON object 
                Return:
                - a dict object so that the caller to this method can use jsonify()
            '''
            message = json.loads(message)
            #logging.info(f"##### {message} is of type {type(message)}")
            msg_type = message["type"]
            match msg_type:
                case "PREPARE":
                    if (self.is_proposer):
                        #logging.info(f"Me a proposer, so rejecting proposal {message['proposal']}")
                        msg = self.acceptor.reject(message["proposal"])
                    else:
                        msg = self.acceptor.on_prepare(message)
                    return msg
                case "ACCEPT":
                    '''
                    message {'type': 'ACCEPT', 
                        'proposal': {'sender_id': '10.10.0.3:8090', 'number': 2}, 
                        'proposed_value': {'shards': {'4e027555-3110-4d23-be4b-7c5815155812': ['10.10.0.2:8090', '10.10.0.4:8090', '10.10.0.7:8090'], '53bc2ea3-dcdc-457c-af55-6d40824ef979': ['10.10.0.6:8090', '10.10.0.5:8090', '10.10.0.3:8090']}}
                    } has type <class 'dict'>
                    '''
                    results: dict = self.acceptor.on_accept(message['proposal'], message['proposed_value'])
                    #logging.info(f"##### ACCEPT {self.address} receives ACCEPT msg {message}")
                    self._populate_shards(message["proposal"], message["proposed_value"])
                    self.is_proposer = False
                    return {"message": "ok"}
                case "ACCEPTED":
                    '''
                    # message: 
                    { "type": "ACCEPTED", 
                    "proposal": {"sender_id": "10.10.0.6:8090", "number": 2}, 
                    "accepted_value": {
                            "shards": {
                                "968fc479-9505-4cfa-933b-8c28e59a0ccc": ["10.10.0.7:8090", "10.10.0.5:8090", "10.10.0.2:8090"], 
                                "d35ed8e4-c113-41a2-84ed-5a868946e665": ["10.10.0.6:8090", "10.10.0.3:8090", "10.10.0.4:8090"]
                            }
                    }
                    } has type <class 'str'>
                    '''
                    #logging.info(f"##### ACCEPTED {self.address} receives ACCEPTED msg {message}")
                    self._populate_shards(message["proposal"], message["accepted_value"])
                    self.is_proposer = False
                    return {"message": "ok"}

        def _populate_shards(self, proposal, value):
            '''
                Update own shard topology and notify KV store of the new
                shard topology

                Parameters:
                - proposal: {"sender_id": "10.10.0.6:8090", "number": 2}, 
                - value": {
                    "shards": {\n
                        "alligator": ["10.10.0.7:8090", "10.10.0.5:8090", "10.10.0.2:8090"],\n 
                        "buffalo": ["10.10.0.6:8090", "10.10.0.3:8090", "10.10.0.4:8090"]
                    }
               
            '''
            # do this so we only process the same Accepted message once
            if not self._start_processing_accepted(proposal):
                return
            shards = value["shards"]
            #s = self.get_shard_members()
            # update the shard topology, for sure
            self.shard_members.clear()
            for shard_id in shards:
                self.shard_members[shard_id] = shards[shard_id]
            #s = self.get_shard_members()
            # see if we have to make changes to our own node KVS
            #to_update = True
            # if there's any old member in the new_members, we don't update
            #for new_member in new_members:
            #    if old_members and new_member in old_members:
            #        to_update = False
            #        break
            if self.notify:
                logging.info(f"[_populate_shards] notify {self.shard_members}")
                #self.kvs.update_shard_info(self.shard_members)
                self.notify("shard_members", self.shard_members)
            self._done_processing_accepted(proposal)

        def _proposal_to_key(self, proposal):
            return f"{proposal['sender_id']}-{proposal['number']}"
        
        def _start_processing_accepted(self, proposal):
            # "proposal": {"sender_id": "10.10.0.6:8090", "number": 2}
            key = self._proposal_to_key(proposal)
            if key not in self.processed_proposals:
                self.processed_proposals[key] = "running"
                return True
            return False
        
        def _done_processing_accepted(self, proposal):
            key = self._proposal_to_key(proposal)
            if key in self.processed_proposals:
                self.processed_proposals[key] = "done"
                return True
            return False
            
        def get(self, case, id=None):
            # {GET} /shard/ids
            if case == "ids":
                if not self.shard_members: time.sleep(2)

                shard_ids = list(self.shard_members.keys())
                return jsonify({"shard-ids": shard_ids }), 200
            
            # {GET} /shard/node-shard-id
            # Retrieve the shard identifier of the responding node, unconditionally
            elif case == "id":               
                for shard_id in self.shard_members:
                    members = self.shard_members[shard_id]
                    if self.address in members:
                        return jsonify({"node-shard-id": shard_id}), 200

                return jsonify({"node-shard-id": None}), 200
            
            # {GET} /shard/members/<id>
            elif case == "members":
                if id not in self.shard_members:
                        return jsonify({"error": f"no such shard ID {id}"}), 404 
                members = self.shard_members[id]   
                return jsonify({"shard-members": members}), 200
            
            elif case == "keycount":
                if id not in self.shard_members:
                    msg = f"no such shard ID {id}"
                    #return jsonify({"error": "Key does not exist"}), 404
                    return jsonify({"error": msg}), 404
                members = self.shard_members[id]
                if self.address in members:
                    results = {}
                    self.notify("kv_size", None, results)
                    key_count = results["kv_size"]
                    #key_count = self.kvs.size()
                    return jsonify({"shard-key-count": key_count}), 200
                else: # forward the request
                    result = None                        
                    for member in members:
                        try:
                            resp = requests.get(f'http://{member}/shard/key-count/{id}', 
                                        headers={'Content-type': 'application/json'}, 
                                        timeout=0.1)
                            result = resp.json()
                            break
                        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                            logging.warning(f"Failed to communicate to {member}")
                    if result:
                        return jsonify(result), 200
                return jsonify({"error": "Failed to communicate to shard {id}"}), 505
                
        def put(self, shard_id: str, addr: str, to_broadcast: False):
            if shard_id not in self.shard_members:
                return jsonify({"error": f"No such shard {shard_id}"}), 404
            if addr not in self.view.view:
                return jsonify({"error": f"{shard_id} not in view"}), 404

            current_addresses = list(self.shard_members.values())
            if addr in current_addresses:
                return jsonify({"error": f"Node {addr} already in shard {shard_id}"}), 400

            destinations = [ x for x in self.view.view if x != self.address ]

            self.shard_members[shard_id].append(addr)
            destinations.append(addr)

            if to_broadcast: # tell other nodes in cluster to make the change
                proposal = { "sender_id": self.address, "number": self.proposer.proposal_number}
                shards = {}
                shards["shards"] = self.shard_members

                payload = {
                    "type": "ACCEPTED",
                    "proposal": proposal,
                    "accepted_value": { "shards": self.shard_members }
                }
                results = self.acceptor.send_accepted(payload, destinations)

            # add the address to the appropriate shard
            if self.notify:
                self.notify("shard_members", self.shard_members)
            #self.kvs.update_shard_info(self.shard_members)    
            return jsonify({"result": "node added to shard"}), 200
            
    class KV_Store:
        def __init__(self, app, view):
            self.app = app
            self.view = view
            self.kvs = {}
            self.address = SOCKET_ADDRESS
            self.shard_id = None
            # shard_members replace views for kvs purposes
            # ordered so that hashing (to index of dict) is consistent
            self.shard_members = {}

            views = self.view.get()[0].json['view']
            
            # views are still necessary to check causal dependency
            self.local_causal_metadata = {replica: 0 for replica in views}

            print(f"Broadcasting replica {SOCKET_ADDRESS} with view {views}")
            # broadcast view to other replicas
            for view in views:
                # print(view)
                if view == SOCKET_ADDRESS: continue

                try:
                    # print(f"Sending PUT /view to socket {view}")
                    #res = requests.put(f'http://{view}/view', json={"socket_address": SOCKET_ADDRESS}, timeout=1)
                    res = requests.put(f'http://{view}/view', json={"socket_address": SOCKET_ADDRESS}, timeout=0.1)

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    print(f"Failed to connect to socket {view}")
            
            # must wait for shards to form before we can update shard memberships

        def _shard_members(self, shard_id=None):
            ''' Get the members in our own shard '''
            if shard_id is None:
                shard_id = self.shard_id
            if self.shard_members and shard_id in self.shard_members:
                return self.shard_members[shard_id]
            return None

        def update_shard_info(self, shard_members: dict, results: dict = None):
            ''' Update other members in this shard '''
            old_shard_members = None
            if self.shard_members:
                old_shard_members = self.shard_members[self.shard_id].copy()
            #old_shard_id = self.shard_id

            self.shard_members.clear()
            self.shard_members = shard_members.copy()

            # update self.shard_id
            for shard_id in self.shard_members:
                if self.address in self.shard_members[shard_id]:
                    self.shard_id = shard_id
                    break

            logging.info(f"##### {self.address} belongs to {self.shard_id} with {self.shard_members[self.shard_id]}")
            for member in self.shard_members[self.shard_id]:
                if member != self.address and self.replicate_kvs(member):                    
                    if self.kvs:
                        break
            logging.info(f"[update_shard_info] KV_Store shards = {self.shard_members}")

            # each member must cleanse its kvs of keys that don't hash into it
            popped = self._cleanse_data()
            logging.info(f"{shard_id}, popped = {popped}")
            for shard_id in popped:
                key_val_pairs = popped[shard_id]
                if key_val_pairs:
                    self._bulk_push(shard_id, key_val_pairs)

            if results:
                results["status"] = "done"

        def _cleanse_data(self):
            popped = {}
            for key in list(self.kvs):
                hashed = self._hash(key)
                if hashed != self.shard_id:
                    if hashed not in popped:
                        popped[hashed] = {}
                    popped[hashed][key] = self.kvs.pop(key)
            return popped

        def _bulk_push(self, shard_id, data: dict):
            destinations = self.shard_members[shard_id]
            for addr in destinations:
                if addr == self.address:
                    continue

                headers = {'Content-type': 'application/json'}
                payload = {
                    "kvs" : data
                }
                try:
                    res = requests.put(f'http://{addr}/kvs/loadAll', headers=headers, json=payload)
                    logging.info(f'##### [replicate_kvs] Recevied data from {addr}: {res.json()}')
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    logging.info(f"Failed to connect to socket {addr}")

        def load_all(self, data):
            logging.info(f"Bulk load into store: {data}")
            if data:
                for k, v in data.items():
                    self.kvs[k] = v
            logging.info(f"kvs: {self.kvs}")

        def replicate_kvs(self, addr):
            ''' Replicate our kv store from addr 
                Return:
                - True if success, False otherwise
            '''
            try:
                res = requests.get(f'http://{addr}/kvs/fetchAll')
                logging.info(f'##### [replicate_kvs] Recevied data from {addr}: {res.json()}')
                kvs, causal_metadata = res.json()['kvs'], res.json()['causal_metadata']
                self.kvs = kvs
                self.local_causal_metadata = causal_metadata
                return True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                logging.info(f"Failed to connect to socket {addr}")
            return False
            
        def size(self, results: dict = None, context=None):
            if results is not None:
                results["kv_size"] = len(self.kvs)
            return len(self.kvs)
        
        def update_view(self, view):
            self.view = view
            views = self.view.get()[0].json['view']
            to_delete = [x for x in self.local_causal_metadata.keys() if x not in views]

            for view in views:
                if view not in self.local_causal_metadata:
                    self.local_causal_metadata[view] = 0
            for view in to_delete:
                del self.local_causal_metadata[view]

        def fetch_all(self):
            return jsonify({
                "kvs": self.kvs,
                "causal_metadata": self.local_causal_metadata
            }), 200

        def check_causal_dependencies(self, sender_addr, incoming_md, is_get=False):
            if is_get:
                return True

            for replica, value in incoming_md.items():
                if replica == sender_addr and incoming_md[replica] != self.local_causal_metadata[replica] + 1:
                    return False
                elif incoming_md[replica] < self.local_causal_metadata[replica]:
                    logging.info(f"[if] replica: {replica}, local md: {self.local_causal_metadata[replica]}, incoming md: {incoming_md[replica]}")
                    return False
            return True

        def _update_causal_metadata(self, sender_addr, incoming_md):
            '''
                if incoming_md is present:
                    - increment the sender's clock count
                    - for the rest, set to max (current, incoming)
                else
                    just increment the sender's clock in our copy
            '''
            # sender_addr = None
            # if 'socket_address' in request:
            #     sender_addr = request['socket_address']

            # if request and 'causal-metadata' in request and request['causal-metadata']:
            #     self._update_causal_metadata(sender_addr, request['causal-metadata'])

            if incoming_md is None:
                self.local_causal_metadata[sender_addr] += 1
            else:
                for addr, value in incoming_md.items():
                    if addr == sender_addr:
                        self.local_causal_metadata[addr] = incoming_md[addr]
                    else:
                        self.local_causal_metadata[addr] = max(self.local_causal_metadata[addr], incoming_md[addr])
            logging.info(f"## updated local md: {self.local_causal_metadata}, incoming md: {incoming_md}")

        def _forward(self, method, addresses, req: dict):
            ''' This is pure forwarding. This node just acts as the go-between 
                of sender and the destination node.
            '''
            destinations = addresses.copy()
            random.shuffle(destinations) # so don't hit the same node over again
            res = None

            # try each of them in the shard (addresses) and hope to hit one
            for addr in destinations: # so we don't overwhelm one node
                if addr == SOCKET_ADDRESS: continue

                # check if replica running
                host = addr.split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)

                connection = sock.connect_ex((host[0], int(host[1])))
                if not connection:
                    # self.app.logger.info(f"Connection to {addr} succeeds")
                    sock.close()

                    forward_url = f'http://{addr}/kvs/{req["key"]}'
                    payload = {"broadcast": True, "causal-metadata": req["causal-metadata"]}

                    if method == 'PUT':
                        logging.info(f"\tAttempting to forward PUT to {forward_url}: {payload}")
                        payload["value"] = req["value"]
                        res = requests.put(forward_url, json=payload, timeout=1)

                    elif method == 'DELETE':
                        logging.info(f"\tAttempting to forward DELETE to {forward_url}: {payload}")
                        res = requests.delete(forward_url, json=payload, timeout=1)

                    elif method == 'GET':
                        logging.info(f"\tAttempting to forward GET to {forward_url}: {payload}")
                        del payload["broadcast"]
                        res = requests.get(forward_url, json=payload, timeout=1)
                        logging.info(f"\tSuccessful foward GET to {forward_url}: {res}")

                    # fowarding to addr success
                    if res and res.status_code:
                        break

            data = res.json()
            '''
            if res.status_code < 300 and 'causal-metadata' in data and data['causal-metadata']:
                self._update_causal_metadata(addr, data['causal-metadata'])
                data['causal-metadata'] = self.local_causal_metadata
            '''
            return {'data': data, 'status_code': res.status_code}
        
        def _hash(self, key):
            ''' Return the shard_id that contains key '''
            # if there's no shard yet, can't do anything
            #logging.info(f"##### [_hash] shards = {self.shard_members}")
            if len(self.shard_members) == 0:
                return None
            n = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
            n_shards = 1
            if self.shard_members:
                n_shards = max(len(self.shard_members), 1)
            idx = n % n_shards
            keys = list(self.shard_members.keys())
            logging.info(f"### keys = {keys}")
            logging.info(f"### [_hash] len(dict) = {len(self.shard_members)}, N={n_shards}, idx = {idx}")
            return keys[idx]
        
        def put(self, key, value, request=None, broadcast=None):
            logging.info(f"Received PUT request:\n\t{request}")

            # Find which shard the key hashes to:
            shard_idx = self._hash(key)
            if shard_idx is None:
                return jsonify({"error": "Shards not yet formed; try again later"}), 503
            logging.info(f"\tKey {key} hashed to shard_idx {shard_idx}, \
                         \n\t\t{self.address}'s (this) shard_id: {self.shard_id}")
            
            members = self.shard_members[shard_idx]

            # Forward request if we don't key doesn't belong to this shard:
            if self.address not in members:
                logging.info(f"\tThis shard {self.shard_id} does NOT own {key}, forwarding to shard {shard_idx}:\n\t\t{payload}")
                # format payload
                payload = {"key": key, "value": value}
                if broadcast: 
                    payload['broadcast'] = broadcast
                if request and "causal-metadata" in request and request['causal-metadata']:
                    payload["causal-metadata"] = request["causal-metadata"]
                
                # forward to shard_idx
                res = self._forward("PUT", members, payload)
                logging.info(f"\tForwarded to {shard_idx}: {res}")
                return res

            logging.info(f"\tKey {key} found in shard, {self.address} processes it. Broadcast: {broadcast}")
            if request and 'causal-metadata' in request and request['causal-metadata']:
                incoming_causal_metadata = request['causal-metadata']
                sender_addr = None
                if 'socket-address' in request and request['socket-address']:
                    sender_addr = request['socket-address']

                if not self.check_causal_dependencies(sender_addr, incoming_causal_metadata):
                    logging.error(f"\tCausal dependencies not satisifed\n\t\tLocal: {self.local_causal_metadata}\n\t\tIncoming: {incoming_causal_metadata}")
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                    
            if len(key) > 50:
                return jsonify({"error": "Key is too long"}), 400

            sender_addr = None
            if 'socket_address' in request:
                sender_addr = request['socket_address']

            # format response
            if key in self.kvs:
                res_body, res_code = "replaced", 200
            else:
                res_body, res_code = "created", 201

            if request and 'causal-metadata' in request and request['causal-metadata']:
                self._update_causal_metadata(sender_addr, request['causal-metadata'])

            # store kv pair
            self.kvs[key] = value
            # delivery action: increment our own clock
            self.local_causal_metadata[SOCKET_ADDRESS] += 1

            # update metadata / broadcast
            #if no_broadcast and 'socket_address' in request:
            #    self.local_causal_metadata[request['socket_address']] += 1
            #else:
            logging.info(f"\tbroadcast = {broadcast} has type {type(broadcast)}")
            if broadcast is None or broadcast is True:
                logging.info(f"\tAttempting to broadcast PUT({key}, {value})")
                self.broadcast('PUT', key, value)
            logging.info(f"\t{self.address} returns: {self.local_causal_metadata}")

            return jsonify({"result": res_body, "causal-metadata": self.local_causal_metadata}), res_code
        
        def get(self, key, incoming_causal_metadata=None):
            # Find which shard the key hashes to, then forward the
            # request if we don't own the key
            shard_idx = self._hash(key)
            if shard_idx is None:
                return jsonify({"error": "Shards not yet formed; try again later"}), 503

            members = self.shard_members[shard_idx]
            if self.address not in members:
                payload = {
                    "key": key,
                    "causal-metadata": incoming_causal_metadata
                }
                logging.info(f"We don't own key {key}, so forwarding to shard {shard_idx}")
                return self._forward("GET", members, payload)

            if incoming_causal_metadata:
                if not self.check_causal_dependencies(None, incoming_causal_metadata, True):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                
            if key not in self.kvs:
                logging.info(f"??????????? GET: {key} NOT in store   ?????? Returning 404")
                return jsonify({"error": "Key does not exist"}), 404
            logging.info(f"GET returns 200: {self.kvs[key]}, {self.local_causal_metadata} ")
            return jsonify({"result": "found", "value": self.kvs[key], "causal-metadata": self.local_causal_metadata}), 200
        
        def delete(self, key, request, no_broadcast=False):
            # Find which shard the key hashes to, then forward the
            # request if we don't own the key
            shard_idx = self._hash(key)
            if shard_idx is None:
                return jsonify({"error": "Shards not yet formed; try again later"}), 503

            members = self.shard_members[shard_idx]
            if self.address not in members:
                payload = {
                    "key": key,
                    "broadcast": no_broadcast                    
                }
                if "causal-metadata" in request:
                    payload["causal-metadata"] = request["causal-metadata"]
                return self._forward("DELETE", members, payload)

            if "causal-metadata" in request:
                incoming_causal_metadata = request['causal-metadata']
                if 'socket-address' in request:
                    sender_addr = request['socket-address']
                if not self.check_causal_dependencies(sender_addr, incoming_causal_metadata):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                
            if key not in self.kvs:
                return jsonify({"error": "Key does not exist"}), 404
            
            # store kv pair
            del self.kvs[key]
            res = jsonify({"result": "deleted", "causal-metadata": self.local_causal_metadata}), 200

            if no_broadcast:
                # update metadata
                self.local_causal_metadata[request['socket_address']] += 1
            else:
                # broadcast
                self.broadcast('DELETE', key, None)
                self.local_causal_metadata[SOCKET_ADDRESS] += 1
                
            return res
            
        def broadcast(self, method, key, value):
            #views = self.view.get()[0].json['view']
            # self.app.logger.info(f"Attempting to broadcast to {views}")
            
            for view in self.shard_members[self.shard_id]:
                if view == SOCKET_ADDRESS: continue
                # self.app.logger.info(f"Attempting to broadcast to {view}")

                # check if replica running
                host = view.split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)

                connection = sock.connect_ex((host[0], int(host[1])))
                if not connection:
                    # self.app.logger.info(f"Connection to {view} succeess")
                    sock.close()
                    url = f'http://{view}/kvs/{key}'
                    payload = {
                        "broadcast": False,
                        "causal-metadata": self.local_causal_metadata,
                        "socket_address": SOCKET_ADDRESS
                    }
                    if method == 'PUT':
                        payload["value"] = value
                        logging.info(f"broadcast {method} to {view}: {payload}")
                        resp = requests.put(url, json=payload, timeout=1)
                        if resp.status_code < 300:
                            self._update_causal_metadata(view, None)
                            logging.info(f"##### broadcast to {view} receives {resp.status_code}, {resp.json()}")
                        else:
                            logging.warning(f"##### Got bad response {resp.status_code} from {view}")
                    elif method == 'DELETE':
                        logging.info(f"broadcast {method} to {view}: {payload}")
                        resp = requests.delete(url, json=payload, timeout=1)
                        if resp.status_code < 300:
                            self._update_causal_metadata(view, None)

                else:
                    # self.app.logger.info(f"Could not connect to {view}")
                    self.shard_members.remove(view)

    class View:
        def __init__(self, app):
            self.app = app
            self.view = []
            if VIEW:
                self.view = VIEW.strip().split(',')

        def put(self, socket_addr):
            # self.app.logger.info(f"Received PUT /view on replica {SOCKET_ADDRESS} for socket {socket_addr}")
            if socket_addr in self.view:
                # self.app.logger.error(f'{socket_addr} already exists on replica {SOCKET_ADDRESS} view')
                return jsonify({"result": "already present"}), 201

            # self.app.logger.info(f'Appending {socket_addr} to views {self.view}')
            self.view.append(socket_addr)
            # self.app.logger.info(f'New view: {self.view}')
            return jsonify({"result": "added"}), 200
        def get(self):
            return jsonify({"view": self.view}), 200
        def delete(self, socket_addr):
            # self.app.logger.info(f'Received DELETE /view on replica {SOCKET_ADDRESS} for socket {socket_addr}')
            if socket_addr not in self.view:
                # self.app.logger.error(f'{socket_addr} does not exist on replica {SOCKET_ADDRESS} view')
                return jsonify({"error": "View has no such replica"}), 404
            
            # self.app.logger.info(f'Current view (before): {self.view}')
            failed_queue = []
            for view in self.view:
                if view == socket_addr: continue
                if view == SOCKET_ADDRESS:
                    idx = self.view.index(socket_addr)
                    # self.app.logger.info(f'Current view (origin): {self.view}')
                    # self.app.logger.info(f'Deleting {socket_addr} on replica {SOCKET_ADDRESS}')
                    self.view.pop(idx)
                    # self.app.logger.info(f'Updated view (origin): {self.view}')
                    continue

                try:
                    requests.delete(f'http://{view}/view', json={'socket_address': socket_addr}, timeout=1)
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    # self.app.logger.error(f'Failed to connect to socket {view}')
                    failed_queue.append(view)

            for view in failed_queue:
                self.delete(view)

            return jsonify({"result": "deleted"}), 200
        

    def run(self, host, port):
        self.app.run(host=host, port=port, debug=True)

if __name__ == '__main__':
    app = Server(__name__)
    app.run(host='0.0.0.0', port=8090)