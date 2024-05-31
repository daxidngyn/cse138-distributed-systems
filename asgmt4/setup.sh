docker kill $(docker ps -q)
docker build -t asg4img .
docker network create --subnet=10.10.0.0/16 asg4net
