echo "# add a new node via 10.0.0.2"
echo '$curl --request PUT --header "Content-Type: application/json" --data ''{"socket-address":"10.10.0.5:8090"}'' http://10.10.0.2:8090/view'
curl --request PUT --header "Content-Type: application/json" --data '{"socket-address":"10.10.0.5:8090"}' http://10.10.0.2:8090/view

echo "# check for the same result on 0.3"
echo '$curl --request GET --header "Content-Type: application/json" http://10.10.0.3:8090/view'
curl --request GET --header "Content-Type: application/json" http://10.10.0.3:8090/view

echo "# delete that added node via 0.4"
echo 'curl --request DELETE --header "Content-Type: application/json" --data ''{"socket-address":"10.10.0.5:8090"}'' http://10.10.0.4:8090/view'
curl --request DELETE --header "Content-Type: application/json" --data '{"socket-address":"10.10.0.5:8090"}' http://10.10.0.4:8090/view

echo "# check the result again on 0.3"
echo '$curl --request GET --header "Content-Type: application/json" http://10.10.0.2:8090/view'
curl --request GET --header "Content-Type: application/json" http://10.10.0.2:8090/view

echo "# take 0.4 off line via deleting 0.4 via 0.2"
echo '$curl --request DELETE --header "Content-Type: application/json" --data ''{"socket-address":"10.10.0.4:8090"}'' http://10.10.0.2:8090/view'
curl --request DELETE --header "Content-Type: application/json" --data '{"socket-address":"10.10.0.4:8090"}' http://10.10.0.2:8090/view

echo "# check the result via 0.3"
echo '$ curl --request GET --header "Content-Type: application/json" http://10.10.0.3:8090/view'
curl --request GET --header "Content-Type: application/json" http://10.10.0.3:8090/view

echo "# check that on 0.4 is off line"
echo '$curl --request GET --header "Content-Type: application/json" http://10.10.0.4:8090/view'
curl --request GET --header "Content-Type: application/json" http://10.10.0.4:8090/view

echo "# now, use 0.2 to bring 0.4 back on line"
echo '$curl --request PUT --header "Content-Type: application/json" --data ''{"socket-address":"10.10.0.4:8090"}'' http://10.10.0.2:8090/view'
curl --request PUT --header "Content-Type: application/json" --data '{"socket-address":"10.10.0.4:8090"}' http://10.10.0.2:8090/view

echo "# check that on 0.4 is back online";
echo '$curl --request GET --header "Content-Type: application/json" http://10.10.0.4:8090/view'
curl --request GET --header "Content-Type: application/json" http://10.10.0.4:8090/view

