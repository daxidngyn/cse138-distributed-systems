
import os
import requests
import socket
from flask import Flask, request, jsonify

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
VIEW = os.environ.get('VIEW')

class Server:
    def __init__(self, name):
        self.app = Flask(name)
        with self.app.app_context():
            self.view = self.View(self.app)
            self.kv_store = self.KV_Store(self.app, self.view)

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
                    # self.app.logger.info(f"Received PUT request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    return self.kv_store.put(key, value, request.json, 'broadcast' in request.json)
                except KeyError:
                    return jsonify({"error": "PUT request does not specify a value"}), 400
            if request.method == 'GET':
                if 'causal-metadata' in request.json:
                    # self.app.logger.info(f"Received GET request on socket {SOCKET_ADDRESS}: {request.json}")
                    return self.kv_store.get(key, request.json['causal-metadata'])
                # self.app.logger.info(f"Received GET request on socket {SOCKET_ADDRESS}")
                return self.kv_store.get(key, None)
            if request.method == 'DELETE':
                try:
                    # self.app.logger.info(f"Received DELETE request on socket {SOCKET_ADDRESS}: {request.json}")
                    value = request.json['value']
                    return self.kv_store.put(key, value, request.json, 'broadcast' in request.json)
                except KeyError:
                    # errors if no causal metadata too
                    return jsonify({"error": "DELETE request does not specify a value"}), 400
                
        @self.app.get('/kvs/fetchAll')
        def kvs_fetch_all():
            return self.kv_store.fetch_all()

    class KV_Store:
        def __init__(self, app, view):
            self.app = app
            self.view = view
            self.kvs = {}

            views = self.view.get()[0].json['view']
            self.local_causal_metadata = {replica: 0 for replica in views}

            print(f"Initializing replica {SOCKET_ADDRESS} with view {views}")
            update_kvs = 1
            # broadcast view to other replicas
            for view in views:
                # print(view)
                if view == SOCKET_ADDRESS: continue

                try:
                    # print(f"Sending PUT /view to socket {view}")
                    res = requests.put(f'http://{view}/view', json={"socket_address": SOCKET_ADDRESS}, timeout=3)
                    
                    if update_kvs:
                        res = requests.get(f'http://{view}/kvs/fetchAll')
                        # print(f'Recevied data from {view}: {res.json()}')
                        kvs, causal_metadata = res.json()['kvs'], res.json()['causal_metadata']
                        self.kvs = kvs
                        self.local_causal_metadata = causal_metadata
                        # self.local_causal_metadata[SOCKET_ADDRESS] = 0
                        update_kvs = 0

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    print(f"Failed to connect to socket {view}")

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

        def check_causal_dependencies(self, incoming_causal_metadata):
            # self.app.logger.info(f"Validating causal metadata {incoming_causal_metadata}")
            for replica, value in incoming_causal_metadata.items():
                if self.local_causal_metadata[replica] != incoming_causal_metadata[replica]:
                    return False
            return True

        def put(self, key, value, request=None, no_broadcast=False):
            if request and 'causal-metadata' in request and request['causal-metadata']:
                incoming_causal_metadata = request['causal-metadata']
                if not self.check_causal_dependencies(incoming_causal_metadata):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                    
            if len(key) > 50:
                return jsonify({"error": "Key is too long"}), 400

            # update metadata / broadcast
            if no_broadcast:
                self.local_causal_metadata[request['socket_address']] += 1
            else:
                self.broadcast('PUT', key, value)
                self.local_causal_metadata[SOCKET_ADDRESS] += 1

            # format response
            if key in self.kvs:
                res = jsonify({"result": "replaced", "causal-metadata": self.local_causal_metadata}), 200
            else:
                res = jsonify({"result": "created", "causal-metadata": self.local_causal_metadata}), 201

            # store kv pair
            self.kvs[key] = value
            
            return res
        
        def get(self, key, incoming_causal_metadata=None):
            if incoming_causal_metadata:
                if not self.check_causal_dependencies(incoming_causal_metadata):
                    return jsonify({"error": "Causal dependencies not satisfied; try again later"}), 503
                
            if key not in self.kvs:
                return jsonify({"error": "Key does not exist"}), 404

            return jsonify({"result": "found", "value": self.kvs[key], "causal-metadata": self.local_causal_metadata}), 200
        
        def delete(self, key, request, no_broadcast=False):
            if "causal-metadata" in request:
                incoming_causal_metadata = request['causal-metadata']
                if not self.check_causal_dependencies(incoming_causal_metadata):
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
            views = self.view.get()[0].json['view']
            # self.app.logger.info(f"Attempting to broadcast to {views}")
            
            for view in views:
                if view == SOCKET_ADDRESS: continue
                # self.app.logger.info(f"Attempting to broadcast to {view}")

                # check if replica running
                host = view.split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)

                connection = sock.connect_ex((host[0], int(host[1])))
                if not connection:
                    # self.app.logger.info(f"Connection to {view} succeess")
                    sock.close()

                    if method == 'PUT':
                        requests.put(f'http://{view}/kvs/{key}', json={
                            "value": value,
                            "broadcast": False,
                            "causal-metadata": self.local_causal_metadata,
                            "socket_address": SOCKET_ADDRESS
                        }, timeout=5)
                    elif method == 'DELETE':
                        requests.delete(f'http://{view}/kvs/{key}', json={
                            "broadcast": False,
                            "causal-metadata": self.local_causal_metadata,
                            "socket_address": SOCKET_ADDRESS
                        }, timeout=5)

                else:
                    # self.app.logger.info(f"Could not connect to {view}")
                    self.view.delete(view)

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