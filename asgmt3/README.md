# CSE138_Assignment3

## Mechanism Descripton

**Describe how your system tracks causal dependencies, and how your system detects when a replica goes down.**

When a new replica is started, it attempts to broadcast its view to other replicas so that the entire system is aware of the new replica. To do so, we attempt to send a `PUT` request to `http://{view}/view` (with its socket address in the body) to every replica:

- if the server fails to connect, we move onto the next replica
- otherwise, the replica receiving this request will add this new replica to its view

In addition, for the first replica that we can successfully reach we fetch its current key-value store and local causal metadata. We can copy this to the current replica in order to match the current state of the system.

### Tracking causal dependencies

The `View` is formatted as an array of strings, with each element being a socket address.

Causal metadata is formatted as a key-value store, where each key is a replica's socket address in the system and the value is its corresponding value. This is representative of a vector clock. Rather than the typical algorithm where a receive event will modify the vector clock, I found that maintaining the same vector clock across all replicas worked better for broadcasting.

The algorithm is as follows:

1. When a replica receives a new write request (`PUT`, `DELETE`), it first checks if causal metadata is present. If so, we can validate causal dependencies by checking if the causal metadata is equal to its local causal metadata. This works since local causal metadata is kept consistent across all replicas in the system.
2. Check if the request is a broadcast message (sent from another replica) or one sent from a client:

   - If the request was sent from a client, we initiate the following broadcast logic (for each replica):
     - Validate if the replica is running by attempting to initiate a socket connection. If this fails, we can delete the replica from our system.
     - Send a request to the replica with the following payload: `{ "broadcast": False, "causal-metadata": <V>, "socket_address": SOCKET_ADDRESS }`
   - Replicas that receive a broadcast request to write will increment their local causal metadata of the origin replica (`socket_address` in the previous payload). Replicas will not send broadcast messages.

3. Finally, we can complete the operation that was specified by the request.

### Replica failure detection

As specified in the above section, replica failure detection occurs when attempting to broadcast, or in any other scenario where a replica needs to communicate with another. We call `DELETE` on `/view` with the replica that has failed.

The initial replica that identifies the failed replica will simply remove the replica from its view. For every other replica (besides the failed one), we can broadcast another `DELETE` on `/view` with the replica that has failed. We also maintain a `failed_queue` of replicas that fail to receive the broadcast, as a way of detecting other replicas that have failed.

- when deleting replicas, we also remove the corresponding socket address from every replica's local causal metadata and view.

Afterwards, we can implement the same logic for the replicas that have failed to receive the `DELETE` broadcast.

## Acknowledgements

N/A

## Citations

- [Flask documentation](https://flask.palletsprojects.com/en/3.0.x/quickstart/)
- [Flask app context](https://flask.palletsprojects.com/en/2.3.x/appcontext/)
- [Vector clocks](https://www.geeksforgeeks.org/vector-clocks-in-distributed-systems/)
- [PerpetualTimer](https://stackoverflow.com/questions/12435211/threading-timer-repeat-function-every-n-seconds)
- [socket.py](https://docs.python.org/3/library/socket.html)
- Various Sites from Python API Documentation

## Team Contributions

David Nguyen:

- create & setup repo + Docker
- initialize `View` class & `Kv_Store` class
- complete `View` class
- implement causal metadata mechanism (tracking causal dependencies, broadcast mechanism, replica failure detection)
- create & write README
- refactor code to pass all tests

Ryan Mac:

- implement all communications among the replicas
- implement causal dependencies and their resolutions
- implement the mechanism through which replicas communicate their liveness to one another
