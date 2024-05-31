import json
from flask import Flask, request
app = Flask(__name__)

@app.route('/ping', methods=['GET', 'PUT'])
def ping():
    if request.method == 'GET':\
        return {'message': "I'm alive!!"}, 200
    else:
        return 'Message Not Allowed', 405
    
@app.route('/ping/<name>', methods=['PUT', 'GET'])
def ping_name(name):
    if request.method == 'PUT':
        return {'message': f"I'm alive, {name}!!"}, 200
    else:
        return 'Method Not Allowed', 405
    
@app.route('/echo', methods=['GET', 'PUT'])
def echo():
    if request.method == 'GET':
        return {'message': 'Get Message Received'}, 200
    if request.method == 'PUT':
        try:
            args = request.get_json()
            json.dumps(args)
            
            if 'message' not in args:
                raise Exception("Body does not contain message key")
        except:
            return "Bad Request", 400
        
        return args, 200
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8085, debug=True)
