from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

class Kv_store:
    def __init__(self):
        self.kv_store = {}
        self.forwarding_address = os.environ.get('FORWARDING_ADDRESS')
        self.main_instance = not self.forwarding_address

    def get_forwarding_request(self, req):
        return f'http://{self.forwarding_address}/kvs/{req}'
    
    def put(self, key, value):
        if not self.main_instance:
            try:
                forwarding_url = self.get_forwarding_request(key)
                res = requests.put(forwarding_url, json={"value": value})
                res_body, res_code = res.json(), res.status_code
                return res_body, res_code
            except:
                return jsonify({"error": "Cannot forward request"}), 503

        if len(key) > 50:
            return jsonify({"error": "Key is too long"}), 400
        
        if key in self.kv_store:
            self.kv_store[key] = value
            return jsonify({"result": "replaced"}), 200
        
        self.kv_store[key] = value
        return jsonify({"result": "created"}), 201
    
    def get(self, key):
        if not self.main_instance:
            try:
                forwarding_url = self.get_forwarding_request(key)
                res = requests.get(forwarding_url)
                res_body, res_code = res.json(), res.status_code
                return res_body, res_code
            except:
                return jsonify({"error": "Cannot forward request"}), 503
        
        if key in self.kv_store:
            return jsonify({"result": "found", "value": self.kv_store[key]}), 200
        
        return jsonify({"error": "Key does not exist"}), 404
    
    def delete(self, key):
        if not self.main_instance:
            try:
                forwarding_url = self.get_forwarding_request(key)
                res = requests.delete(forwarding_url)
                res_body, res_code = res.json(), res.status_code
                return res_body, res_code
            except:
                return jsonify({"error": "Cannot forward request"}), 503
        
        if key in self.kv_store:
            del self.kv_store[key]
            return jsonify({"result": "deleted"}), 200
        else:
            return jsonify({"error": "Key does not exist"}), 404

kv_store = Kv_store()

# PUT request implementation for /kvs/<key>
@app.route('/kvs/<key>', methods=['PUT'])
def put_key_value(key):
    try:
        # reads value from JSON request body   
        value = request.json['value']
        return kv_store.put(key, value)
    except KeyError:
        return jsonify({"error": "PUT request does not specify a value"}), 400

# GET request implementation for /kvs/<key>
@app.route('/kvs/<key>', methods=['GET'])
def get_key_value(key):
    return kv_store.get(key)

# delete implementation for /kvs/<key>
@app.route('/kvs/<key>', methods=['DELETE'])
def delete_key_value(key):
    return kv_store.delete(key)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090, debug=True)