Summary:
A Python Flask HTTP web service providing basic Key-Value Store functionality. Supports storing, retrieving,
and deleting key-value pairs with an optional forwarding mechanism.


Usage:
To build the docker image, run:
$ docker build -t asg2img .

Now, create a subnet:
$ docker network create --subnet=10.10.0.0/16 asg2net

Run main instance:
$docker run --rm -d -p 8082:8090 --net=asg2net --ip=10.10.0.2 --name main-instance asg2img

Run forwarding proxy:
$ docker run --rm -d -p 8083:8090 --net=asg2net --ip=10.10.0.3 -e FORWARDING_ADDRESS=10.10.0.2:8090 --name forwarding-instance1 asg2img


Team Contributions:
Renata Lopez - Created Github repo and added support for GET, PUT, and DELETE requests to the key-value store with initial push.
David Nguyen - Initiated group communication and implemented kv_store class to provide functionality for forwarding instances.
Ryan Mac - Wrote documentation provided example for assignment in another programming language (Nodejs)

Acknowledgements: N/A

Citations: N/A
