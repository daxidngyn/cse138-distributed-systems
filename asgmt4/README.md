# CSE138_Assignment4

## Mechanism Description

### Tracking causal dependencies

- describe how system tracks causal dependencies (can be same as assignment 3)
- describe how system detects when a node goes down (can be same as assignment 3)
- describe how to shard keys across nodes:
  - approach to divide nodes into shards
  - approach to map keys-to-shards

Tracking causal dependencies are implemented in the same way as Assignment 3. From Assignment 3:

1. When a replica receives a new write request (`PUT`, `DELETE`), it first checks if causal metadata is present. If so, we can validate causal dependencies by checking if the causal metadata is equal to its local causal metadata. This works since local causal metadata is kept consistent across all replicas in the system.
2. Check if the request is a broadcast message (sent from another replica) or one sent from a client:

   - If the request was sent from a client, we initiate the following broadcast logic (for each replica):
     - Validate if the replica is running by attempting to initiate a socket connection. If this fails, we can delete the replica from our system.
     - Send a request to the replica with the following payload: `{ "broadcast": False, "causal-metadata": <V>, "socket_address": SOCKET_ADDRESS }`
   - Replicas that receive a broadcast request to write will increment their local causal metadata of the origin replica (`socket_address` in the previous payload). Replicas will not send broadcast messages.

3. Finally, we can complete the operation that was specified by the request.

### Replica failure detection

Replica failure detection is implemented in the same way as Assignment 3. From Assignment 3:

Replica failure detection occurs when attempting to broadcast, or in any other scenario where a replica needs to communicate with another. We call `DELETE` on `/view` with the replica that has failed.

The initial replica that identifies the failed replica will simply remove the replica from its view. For every other replica (besides the failed one), we can broadcast another `DELETE` on `/view` with the replica that has failed. We also maintain a `failed_queue` of replicas that fail to receive the broadcast, as a way of detecting other replicas that have failed.

- when deleting replicas, we also remove the corresponding socket address from every replica's local causal metadata and view.

Afterwards, we can implement the same logic for the replicas that have failed to receive the `DELETE` broadcast.

### Sharding keys across nodes

**Each shard must contain at least two nodes to provide fault tolerance**.

We use simple hashing `MD5(key) % n` where `n = SHARD_COUNT`. We then map this to the corresponding `shard_id` (sorted collection).

When a reshard occurs (`SHARD_COUNT` changes), we begin to redistribute the nodes in the new shard topology. The redistribution attempts to spread the nodes (somewhat evenly) among the shards while minimizing the (re)assignment of nodes in old shards. We use the following logic:

- each shard should have `total_number_of_nodes / shard_count` members to start
  - if extra, each iteration will consume one until no more extra
- the redistribution will not take place if it cannot meet the requirement of at least 2 nodes per shard.

After a reshard, each node will _cleanse_ its own data store, such that it only keeps the keys that hash to its `shard_id` while forwarding the rest to the appropriate shards.

## Acknowledgements

N/A

## Citations

[Paxos: Tom Cocagne](https://github.com/cocagne/paxos/) Our Paxos implementation (paxos.py) is based on the ideas from this GitHub repository. Although, our implementation is much more simple as we followed the algorithm as explained in class.

## Team Contributions

David Nguyen:

- implemented foundation for replicated, fault-tolereant, causally consitent kv store (from assignment 3)

Ryan Mac:

- create & add assignment dependencies
- implemented sharding operations


where <node_name> is one of those names (alice, bob, carol, dave, erin, frank, and grace if you want to do the add-member stuff)
