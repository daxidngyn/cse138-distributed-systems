docker kill $(docker ps -q)
docker build -t asg4img .

docker run -d --rm -p 8084:8090 --net=asg4net --ip=10.10.0.4 --name=carol -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.4:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img

docker run -d --rm -p 8082:8090 --net=asg4net --ip=10.10.0.2 --name=alice -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.2:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img

docker run -d --rm -p 8083:8090 --net=asg4net --ip=10.10.0.3 --name=bob -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.3:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img

docker run -d --rm -p 8085:8090 --net=asg4net --ip=10.10.0.5 --name=dave -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.5:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img

docker run -d --rm -p 8087:8090 --net=asg4net --ip=10.10.0.7 --name=frank -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.7:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img 

docker run --rm -p 8086:8090 --net=asg4net --ip=10.10.0.6 --name=erin -e=SHARD_COUNT=2 \
-e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.6:8090 \
-e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090 \
asg4img


# docker run -d --rm -p 8088:8090 --net=asg4net --ip=10.10.0.8 --name=grace -e=SHARD_COUNT=2 -e=DEBUG=1 -e=SOCKET_ADDRESS=10.10.0.8:8090 -e=VIEW=10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090,10.10.0.5:8090,10.10.0.6:8090,10.10.0.7:8090,10.10.0.8:8090 asg4img 
