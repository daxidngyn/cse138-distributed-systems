
import os
import requests
import socket
import time
import hashlib
from flask import Flask, request, jsonify

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
VIEW = os.environ.get('VIEW')

SHARD_COUNT = os.environ.get('SHARD_COUNT')

class Server:
    def __init__(self, name):
        self.app = Flask(name)
        with self.app.app_context():
            self.view = self.View(self.app)
            self.kv_store = self.KV_Store(self.app, self.view, self.node_alive)
            self.shard = self.Shard(self.app, self.view, self.kv_store, self.kvs_ping, self.node_alive)

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
                    return jsonify({"error": "PUT request does not specify a value"}), 400
            if request.method == 'GET':
                return self.view.get()
            if request.method == 'DELETE':
                try:
                    socket_addr = request.json['socket_address']          
                    res = self.view.delete(socket_addr)
                    self.kv_store.update_view(self.view)
                    return res
                except KeyError:
                    return jsonify({"error": "PUT request does not specify a value"}), 400
                
        @self.app.route('/kvs/<key>', methods=['PUT', 'GET', 'DELETE'])
        def kvs_api(key):
            if request.method == 'PUT':
                try:
                    self.app.logger.info(f"Received PUT request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    return self.kv_store.put(key, value, request.json, 'broadcast' not in request.json)
                except KeyError:
                    return jsonify({"error": "PUT request does not specify a value"}), 400
            if request.method == 'GET':
                if 'causal-metadata' in request.json:
                    self.app.logger.info(f"Received GET request on socket {SOCKET_ADDRESS}: {request.json}")
                    return self.kv_store.get(key, request.json['causal-metadata'], 'broadcast' not in request.json)
                # self.app.logger.info(f"Received GET request on socket {SOCKET_ADDRESS}")
                return self.kv_store.get(key, None)
            if request.method == 'DELETE':
                try:
                    self.app.logger.info(f"Received DELETE request on socket {SOCKET_ADDRESS}: {request.json}")
                    return self.kv_store.delete(key, request.json, 'broadcast' not in request.json)
                except KeyError:
                    # errors if no causal metadata too
                    return jsonify({"error": "DELETE request does not specify a value"}), 400
                
        @self.app.get('/kvs/fetchAll')
        def kvs_fetch_all():
            return self.kv_store.fetch_all()
        
        # update kvs and causal metadata of kvs
        @self.app.put('/kvs/replace')
        def kvs_replace():
            new_kvs = request.json['new_kvs']
            new_causal_metadata = request.json['new_causal_metadata']

            self.kv_store.replace_kvs_data(new_kvs, new_causal_metadata)
            return jsonify({"success": True}), 201
            
        @self.app.route('/kvs/receive-broadcast/<key>', methods=['PUT', 'DELETE'])
        def kvs_receive_broadcast(key):
            if request.method == 'PUT':
                try:
                    self.app.logger.info(f"Received PUT broadcast request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    causal_metadata = request.json['causal-metadata']
                    update_kvs = request.json['update-kvs']
                    if update_kvs:
                        # run the kvs operation, which in turn updates local causal metadata too
                        return self.kv_store.put(key, value, request.json, 'broadcast' in request.json)
                    else:
                        # ONLY update local causal metadata of node
                        self.kv_store.replace_kvs_data(None, causal_metadata)
                        return jsonify({"success": True}), 201
                except KeyError:
                    return jsonify({"error": "PUT /kvs/receive-broadcast request does not specify a value"}), 400
            elif request.method == 'DELETE':
                try:
                    self.app.logger.info(f"Received DELETE broadcast request on socket {SOCKET_ADDRESS}: {request.json}")
                    causal_metadata = request.json['causal-metadata']
                    update_kvs = request.json['update-kvs']
                    
                    if update_kvs:
                        return self.kv_store.delete(key, request.json, 'broadcast' in request.json)
                    else:
                        self.kv_store.replace_kvs_data(None, causal_metadata)
                        return jsonify({"success": True}), 201
                except KeyError:
                    return jsonify({"error": "DELETE /kvs/receive-broadcast request does not specify a value"}), 400
            
        # assignment 4 /shard
        @self.app.route('/shard/ids')
        def shard_query_all():
            self.app.logger.info(f"Received GET /shards/ids at address {SOCKET_ADDRESS}")
            return self.shard.get("ids")
        
        @self.app.get('/shard/node-shard-id')
        def shard_query_one():
            self.app.logger.info(f"Received GET /shard/node-shard-id at address {SOCKET_ADDRESS}")
            return self.shard.get("id")
        
        @self.app.get('/shard/members/<id>')
        def shard_members(id):
            self.app.logger.info(f"Received GET /shard/members/{id} at address {SOCKET_ADDRESS}")
            return self.shard.get("members", id)
        
        @self.app.get('/shard/key-count/<id>')
        def shard_count(id):
            self.app.logger.info(f"Received GET /shard/key-count/{id} at address {SOCKET_ADDRESS}")
            return self.shard.get("keycount", id)
        
        @self.app.put('/shard/add-member/<id>')
        def shard_add_member(id):
            self.app.logger.info(f"Received PUT /shard/add-member/{id} at address {SOCKET_ADDRESS}")
            try:
                return self.shard.put_add_member(id, request.json["socket-address"], 'broadcast' not in request.json)
            except KeyError:
                self.app.logger.error(f"KeyError: PUT /shard/add-member: {request.json}")
                return jsonify({"error": "PUT /shard/add-member must specify 'socket-address' in body"}), 404
            
        @self.app.put('/shard/reshard')
        def reshard():
            self.app.logger.info(f"Received PUT /shard/reshard at address {SOCKET_ADDRESS}")
            try:
                shard_count = int(request.json["shard-count"])
                if shard_count < 0:
                    return jsonify({"error": "'shard-count' in body must be positive integer"}), 404
                return self.shard.put_reshard(shard_count)
            except KeyError:
                return jsonify({"error": "must specify 'shard-count' in body"}), 404
            except ValueError:
                return jsonify({"error": "'shard-count' in body must be positive integer"}), 404

        @self.app.put('/shard/update-shard-members')
        def update_shard_members():
            self.app.logger.info(f"Received PUT /shard/update-shard-members at address {SOCKET_ADDRESS}")
            try:
                shard_members = request.json["shard_members"]
                self.shard.update_shard_members(shard_members)
                return jsonify({"success": True}), 201
            except KeyError:
                return jsonify({"error": "must specify 'shard-count' in body"}), 404
            
    def node_alive(self, addr):
        host = addr.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        connection = sock.connect_ex((host[0], int(host[1])))
        if not connection:
            sock.close()
            return True
        else:
            print(f"Could not connect to replica {addr}")
            return False

    def kvs_ping(self, type: str, context: dict):
        assert type in ('kvs_shard_members', 'kv_size')
        if type == "kvs_shard_members":
            self.kv_store.replace_kvs_data(None, None, context["shard_members"])
        elif type == "kv_size":
            return self.kv_store.ping_kvs_size()

    class Shard:
        def __init__(self, app, view, kvs, callback_fn=None, node_alive=None):
            self.app = app
            self.view = view
            self.kvs = kvs
            self.ping_kvs = callback_fn
            self.node_alive = node_alive

            # mapping of shard ID -> nodes (SOCKET_ADDRESS) in shard
            self.shard_members = {}

            if SHARD_COUNT:
                # initial startup
                self.shard_count = int(SHARD_COUNT)
            else:
                # new node; should join view as usual but not participate in any shard
                # until receiving /shard/add-member/<ID>
                self.shard_count = 0

            # initialize shards with replica nodes on startup
            if self.shard_count:
                print(f"Broadcasting distribution to replicas...")
                view = self.view.get()[0].json['view']
                num_nodes = len(view)
                remainder = num_nodes % self.shard_count
                nodes_per_shard = int(num_nodes / self.shard_count)

                for i in range(self.shard_count):
                    shard_id = f's{i}'
                    shard_members = []

                    for _ in range(nodes_per_shard):
                        node = view.pop(0)
                        shard_members.append(node)
                    if remainder:
                        node = view.pop(0)
                        shard_members.append(node)
                        remainder -= 1

                    self.shard_members[shard_id] = shard_members

                self.ping_kvs('kvs_shard_members', {'shard_members': self.shard_members})
                print(f"Initialized shards with nodes:\n\t{self.shard_members}")

        def update_shard_members(self, shard_members):
            self.shard_members = shard_members
            self.shard_count = len(list(shard_members).keys())
            # update shard_members on kvs
            self.ping_kvs('kvs_shard_members', {'shard_members': shard_members})

        def distribute_nodes_shards(self, shard_count, nodes_per_shard, remainder):
            def create_new_shard(prev_data):
                shard_members = []
                update_q = {}
                kv_data = prev_data.pop(0)
                for _ in range(nodes_per_shard):
                    node = nodes.pop(0)
                    if node_to_shard_map[node] == kv_data['shard']: continue
                    update_q[node] = kv_data
                    shard_members.append(node)
                
                return {"shard_members": shard_members, "update_q": update_q}
        
            view = self.view.get()[0].json['view']
            # maps nodes -> shards
            node_to_shard_map = {}
            for shard_id, nodes in self.shard_members.items():
                for node in nodes:
                    node_to_shard_map[node] = shard_id

            # get data of previous replicas in shards
            prev_data = []
            for shard in self.shard_members:
                for node in self.shard_members[node]:
                    res = requests.get(f'http://{view}/kvs/fetchAll')
                    kvs, causal_metadata = res.json()['kvs'], res.json()['causal_metadata']
                    prev_data.append({"shard": shard, "kvs": kvs, "causal_metadata": causal_metadata})
                    break

            new_shard_members = {}
            update_q = {}
            for i in range(shard_count):
                shard_id = f's{i}'
                shard_members = []
                kv_data = prev_data.pop(0)

                for _ in range(nodes_per_shard):
                    node = nodes.pop(0)
                    # same data, no need to update kvs
                    if node_to_shard_map[node] == kv_data['shard']: continue

                    # add to update queue, update data later
                    update_q[node] = kv_data
                    shard_members.append(node)

                if remainder:
                    node = nodes.pop(0)
                    if node_to_shard_map[node] == kv_data['shard']: continue
                    shard_members.append(node)
                    remainder -= 1

                new_shard_members[shard_id] = shard_members
                
            # idk if needed
            if len(prev_data) > 0:
                shard_id = f's{shard_count}'
                self.app.logger.info(f"\tAdding new shard {shard_id} to prevent data loss")
                new_shard_data = create_new_shard(prev_data)
                new_shard_members[shard_id] = new_shard_data['shard_members']
                update_q.update(new_shard_data['update_q'])
                
            self.app.logger.info(f"\tReplicas to update:\n\t\t{update_q}")
            # update replicas
            for node, data in update_q.items():
                self.app.logger.info(f"\t\tAttempting to update node {node}...")
                kvs, causal_metadata = data['kvs'], data['causal_metadata']
                try:
                    requests.put(f'http://{node}/kvs/replace', json={
                        "new_kvs": kvs,
                        "new_causal_metadata": causal_metadata
                    }, timeout=3)
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    print(f"Failed to connect to socket {view}")

            # broadcast new shard members
            for node in view:
                if node == SOCKET_ADDRESS: continue
                try:
                    self.app.logger.info("Attempting to broadcast new shard members to {node}")
                    requests.put(f'http://{node}/shard/update-shard-members', json={
                        "shard_members": new_shard_members
                    }, timeout=3)
                except:
                    self.app.logger.error(f"Error broadcasting new shard members to {node}")

            self.app.logger.info(f"\tRedistribution success. New shard members:\n\t\t{new_shard_members}")

        def put_add_member(self, shard_id: str, node: str, broadcast=False):
            views = self.view.get()[0].json['view']
            self.app.logger.info(f'{SOCKET_ADDRESS} view: {views}')

            if shard_id not in self.shard_members:
                self.app.logger.error(f"Shard id <{shard_id}> does not exist")
                return jsonify({"error": f"Shard id <{shard_id}> does not exist"}), 404
            if node not in views:
                self.app.logger.error(f"Shard id <{shard_id}> not in view")
                return jsonify({"error": f"Shard id <{shard_id}> not in view"}), 404
            
            current_nodes = list(self.shard_members.values())
            for nodes in current_nodes:
                if node in nodes:
                    return jsonify({"error": f"Node {node} already exists"}), 400
            
            if broadcast:
                # broadcast new shard mapping to all other nodes
                self.app.logger.info(f'\tInitiating /shard/add-member/{shard_id} request to {views}')
                for view in views:
                    if view == SOCKET_ADDRESS or view == node: continue

                    try:
                        self.app.logger.info(f"\tAttempting to add node {node} to node {view}'s shard members...")
                        requests.put(f'http://{view}/shard/add-member/{shard_id}', json={
                            "socket-address": node,
                            "broadcast": False
                        }, timeout=3)
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                        print(f"Failed to connect to socket {view}")

                # update data of new node
                try:
                    self.app.logger.info(f'\tAttempting to update {node} using {shard_id} replicas: {self.shard_members[shard_id]}')
                    for n in self.shard_members[shard_id]:
                        if n == node: continue

                        self.app.logger.info(f'\tAttempting to fetch KVS data from {n}...')
                        res = requests.get(f'http://{n}/kvs/fetchAll')
                        kvs, causal_metadata = res.json()['kvs'], res.json()['causal_metadata']
                        self.app.logger.info(f'\tRetrieved KVS data from {n}')

                        requests.put(f'http://{node}/kvs/replace', json={
                                        "new_kvs": kvs,
                                        "new_causal_metadata": causal_metadata,
                                    }, timeout=3)

                        new_shard_members = self.shard_members.copy()
                        new_shard_members[shard_id].append(node)
                        requests.put(f'http://{node}/shard/update-shard-members', json={
                            "shard_members": new_shard_members
                        }, timeout=3)
                        break
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                        print(f"Failed to connect to socket {view}")

                # self.put_reshard()

            # assign node to shard_id
            self.shard_members[shard_id].append(node)
            self.app.logger.info(f'\tAssigned node {node} to shard {shard_id}: {self.shard_members}')

            return jsonify({"result": "node added to shard"}), 200
        
        def put_reshard(self, shard_count):
            nodes = []
            for node_lst in self.shard_members.values():
                nodes.extend(node_lst)

            num_nodes = len(nodes)
            if num_nodes < 2 * shard_count:
                return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400

            nodes = []
            for node_lst in self.shard_members.values():
                nodes.extend(node_lst)

            num_nodes = len(nodes)
            remainder = num_nodes % shard_count
            nodes_per_shard = int(num_nodes / shard_count)

            self.distribute_nodes_shards(shard_count, nodes_per_shard, remainder)

            # Trigger a reshard into <shard_count> shards, maintaining fault-tolerance invariant 
            # that each shard contains at least two nodes

            # reshard when:
            # if shard contains only one node (due to failure of other nodes)
            # add new nodes to store (redistributing)

            # rebalance keys !!!!

            return jsonify({"result": "resharded"})

        def get(self, case, id=None):
            # {GET} /shard/ids
            # Retrieve the list of all shard identifiers, unconditionally
            if case == "ids":
                if not self.shard_members: 
                    time.sleep(2)

                shard_ids = list(self.shard_members.keys())
                return jsonify({"shard-ids": shard_ids }), 200
            
            # {GET} /shard/node-shard-id
            # Retrieve the shard identifier of the responding node, unconditionally
            elif case == "id":
                for shard_id in self.shard_members:
                    members = self.shard_members[shard_id]
                    self.app.logger.info(f"{shard_id}: {members}")
                    if SOCKET_ADDRESS in members:
                        return jsonify({"node-shard-id": shard_id}), 200

                self.app.logger.error(f"\tCould not find shard id of {SOCKET_ADDRESS}")
                return jsonify({"node-shard-id": None}), 200
            
            # {GET} /shard/members/<id>
            # Look up the members of the indicated shard
            elif case == "members":
                if id not in self.shard_members:
                        return jsonify({"error": f"Shard id <{id}> does not exist"}), 404 
                members = self.shard_members[id]
                return jsonify({"shard-members": members}), 200
            
            # {GET} /shard/key-count/<id>
            elif case == "keycount":
                if id not in self.shard_members:
                    return jsonify({"error": f"Shard id <{id}> does not exist"}), 404
                
                if SOCKET_ADDRESS not in self.shard_members[id]:
                    self.app.logger.info(f"\tNode {SOCKET_ADDRESS} does not belong to shard {id}. Attempting to forward request...")
                    for node in self.shard_members[id]:
                        try:
                            self.app.logger.info(f"\tForwarding /shard/key-count/{id} to node {node}...")
                            res = requests.get(f'http://{node}/shard/key-count/{id}', timeout=3)
                            key_count = res.json()['shard-key-count']
                            break
                        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                            self.app.logger.error(f"Failed to connect to node {node}")
                else:
                    key_count = self.ping_kvs('kv_size', None)

                return jsonify({"shard-key-count": key_count}), 200

    class KV_Store:
        def __init__(self, app, view, node_alive):
            self.app = app
            self.view = view
            self.node_alive = node_alive
            self.kvs = {}

            views = self.view.get()[0].json['view']
            self.local_causal_metadata = {replica: 0 for replica in views}

            # keeps track of all shards
            self.shard_members = {}

            print(f"Initializing replica {SOCKET_ADDRESS} with view {views}")
            # broadcast view to other replicas
            for view in views:
                # print(view)
                if view == SOCKET_ADDRESS: continue

                try:
                    # print(f"Sending PUT /view to socket {view}")
                    requests.put(f'http://{view}/view', json={"socket_address": SOCKET_ADDRESS}, timeout=3)

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    print(f"Failed to connect to socket {view}")

        # hash to determine which shard a key belongs in
        def hash_fn(self, key):
            if len(self.shard_members) == 0: return None

            hashed_key = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
            n = len(list(self.shard_members.keys()))
            
            return hashed_key % n

        def ping_kvs_size(self):
            kv_size = len(list(self.kvs.keys()))
            return kv_size
        
        def replace_kvs_data(self, new_kvs=None, new_causal_metadata=None, shard_members=None):
            self.app.logger.info(f"\tUpdating {SOCKET_ADDRESS} data...")
            if new_kvs:
                self.kvs = new_kvs
            if new_causal_metadata:
                self.local_causal_metadata = new_causal_metadata
            if shard_members:
                self.shard_members = shard_members

        def put(self, key, value, request=None, broadcast=False):
            shard_idx = self.hash_fn(key)
            if shard_idx is None:
                return jsonify({"error": "Shards not yet formed; try again later"}), 503

            # determine if local or remote key
            shard_id = list(self.shard_members.keys())[shard_idx]
            shard_members = self.shard_members[shard_id]

            local_key = SOCKET_ADDRESS in shard_members
            if local_key:
                self.app.logger.info(f"\tLocal key found on shard {shard_id} with nodes {shard_members}")
                if request and 'causal-metadata' in request and request['causal-metadata']:
                    incoming_causal_metadata = request['causal-metadata']
                    if not self.check_causal_dependencies(incoming_causal_metadata):
                        self.app.logger.error(f"\tCausal dependencies not satisfied")
                        return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                    
                self.app.logger.info(f"\tCausal dependencies satisfied. Attempting to insert ({key}, {value})")
    
                if len(key) > 50:
                    return jsonify({"error": "Key is too long"}), 400

                # update metadata / broadcast
                if broadcast:
                    self.broadcast('PUT', key, value, shard_members)
                    self.local_causal_metadata[SOCKET_ADDRESS] += 1
                else:
                    self.local_causal_metadata[request['socket_address']] += 1
                    
                # format response
                if key in self.kvs:
                    res_result, res_status = "replaced", 200
                else:
                    res_result, res_status = "created", 201
                
                # store kv pair
                self.kvs[key] = value
                self.app.logger.info(f"\tSuccesfully stored ({key}, {value}) at {shard_id}")
                res_causal_metadata = self.local_causal_metadata
                
            else: # remote key
                self.app.logger.info(f"\tRemote key found on shard {shard_id} with nodes {shard_members}")
                # forward request to member of that shard
                causal_metadata = request['causal-metadata'] if request and 'causal-metadata' in request else None
                self.app.logger.info(f"Forwarding PUT({key}, {value}) to {shard_members[0]}...")
                res = requests.put(f'http://{shard_members[0]}/kvs/{key}', json={
                            "value": value,
                            "causal-metadata": causal_metadata,
                        }, timeout=3)
                res_result, res_causal_metadata, res_status = res.json()['result'], res.json()['causal-metadata'], res.status_code
                self.app.logger.info(f"\tSuccesfully forwarded ({key}, {value}) to {shard_members[0]}: {res.json()}")

            return jsonify({"result": res_result, "causal-metadata": res_causal_metadata}), res_status
                
        def get(self, key, incoming_causal_metadata=None, broadcast=False):
            if incoming_causal_metadata:
                if not self.check_causal_dependencies(incoming_causal_metadata):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                
            if key in self.kvs:
                return jsonify({"result": "found", "value": self.kvs[key], "causal-metadata": self.local_causal_metadata}), 200
            
            # if key not in this node, search other shards
            if broadcast:
                for shard in list(self.shard_members.keys()):
                    if SOCKET_ADDRESS in shard: continue
                    for node in self.shard_members[shard]:
                        try:
                            self.app.logger.info(f'Attempting to fetch key {key} at node {node}...')
                            res = requests.get(f'http://{node}/kvs/{key}', json={'causal-metadata': self.local_causal_metadata, 'broadcast': False})
                            if res.status_code == 200:
                                self.app.logger.info(f'Success: {res.json()}')
                                return jsonify({"result": "found", "value": res.json()['value'], "causal-metadata": res.json()['causal-metadata']}), 200
                            # only need to search one node in each shard
                            else: break
                        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                            self.app.logger.error(f"Failed to connect to {node}")

            return jsonify({"error": "Key does not exist"}), 404

        def delete(self, key, request, broadcast=False):
            if "causal-metadata" in request:
                incoming_causal_metadata = request['causal-metadata']
                if not self.check_causal_dependencies(incoming_causal_metadata):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                
            if key not in self.kvs:
                return jsonify({"error": "Key does not exist"}), 404
            
            # store kv pair
            del self.kvs[key]
            res = jsonify({"result": "deleted", "causal-metadata": self.local_causal_metadata}), 200

            if broadcast:
                self.broadcast('DELETE', key, None)
                self.local_causal_metadata[SOCKET_ADDRESS] += 1
            else:
                # update metadata
                self.local_causal_metadata[request['socket_address']] += 1
                
            return res
        
        def broadcast(self, method, key, value, shard_members):
            views = self.view.get()[0].json['view']
            self.app.logger.info(f"\tReceived broadcast request to {views}...")
            
            for view in views:
                if view == SOCKET_ADDRESS: continue
                self.app.logger.info(f"\tAttempting to broadcast to {view}...")

                # check if replica running
                if not self.node_alive(view):
                    self.app.logger.info(f"\tCould not connect to {view}")
                    self.view.delete(view)
                    continue

                self.app.logger.info(f"\tConnection to {view} succeess")
                # determine if we want to broadcast an update to
                # ONLY causal_metadata (nodes outside shard) or
                # BOTH kvs and causal_metadata (nodes in same shard)
                causal_metadata = self.local_causal_metadata.copy()
                causal_metadata[SOCKET_ADDRESS] += 1
                update_kvs = view in shard_members

                if method == 'PUT':
                    requests.put(f'http://{view}/kvs/receive-broadcast/{key}', json={
                        "value": value,
                        "broadcast": False,
                        "update-kvs": view in shard_members,
                        "causal-metadata": self.local_causal_metadata if update_kvs else causal_metadata,
                        "socket_address": SOCKET_ADDRESS
                    }, timeout=5)
                elif method == 'DELETE':
                    requests.delete(f'http://{view}/kvs/receive-broadcast/{key}', json={
                        "broadcast": False,
                        "update-kvs": view in shard_members,
                        "causal-metadata": self.local_causal_metadata if update_kvs else causal_metadata,
                        "socket_address": SOCKET_ADDRESS
                    }, timeout=5)

        def check_causal_dependencies(self, incoming_causal_metadata):
            # self.app.logger.info(f"Validating causal metadata {incoming_causal_metadata}")
            for replica, value in incoming_causal_metadata.items():
                if self.local_causal_metadata[replica] != incoming_causal_metadata[replica]:
                    return False
            return True
        
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
                    requests.delete(f'http://{view}/view', json={'socket_address': socket_addr}, timeout=3)
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