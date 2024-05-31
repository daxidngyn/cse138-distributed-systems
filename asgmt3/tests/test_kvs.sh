echo "# PUT(x,1) via 10.10.0.2"
echo '$curl --request PUT --header "Content-Type: application/json" --write-out "\\n%{http_code}\\n" --data ''{"value":1,"causal-metadata":null}'' http://10.10.0.2:8090/kvs/x'
echo ""
echo "Response:\n"
curl --request PUT --header "Content-Type: application/json" --write-out "\n%{http_code}\n" --data '{"value":1,"causal-metadata":null}' http://10.10.0.2:8090/kvs/x

sleep 1

echo "----------------------------------------------"
echo "# GET(x) via 10.10.0.3, not aware of PUT(x,1), thus no causal-metadata"
echo '$curl --request GET --header "Content-Type: application/json" --write-out "\\n%{http_code}\\n" http://10.10.0.3:8090/kvs/x'
echo ""
echo "Response:\n"
curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n" http://10.10.0.3:8090/kvs/x

echo "----------------------------------------------"
echo "# GET(x) via 10.10.0.2, with previously received causal-metadata"
echo '$curl --request GET --header "Content-Type: application/json" \
--data ''{ "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}'' \
--write-out "\\n%{http_code}\\n" http://10.10.0.2:8090/kvs/x'
echo ""
echo "Response:\n"
curl --request GET --header "Content-Type: application/json" \
--data '{ "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}' \
--write-out "\n%{http_code}\n" http://10.10.0.2:8090/kvs/x

echo "----------------------------------------------"
echo "# PUT(y, 2) via  10.0.0.3. NOTICE the vector clock changes!!!"
echo '$curl --request PUT --header "Content-Type: application/json" \
--data ''{"value": 2, "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}'' \
--write-out "\\n%{http_code}\\n" http://10.10.0.3:8090/kvs/y'
echo ""
echo "Response:\n"
curl --request PUT --header "Content-Type: application/json" \
--data '{"value": 2, "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}' \
--write-out "\n%{http_code}\n" http://10.10.0.3:8090/kvs/y

sleep 1

echo "----------------------------------------------"
echo "# GET(y) (should be 2) via 10.10.0.4"
echo '$curl --request GET --header "Content-Type: application/json" --write-out "\\n%{http_code}\\n" http://10.10.0.4:8090/kvs/y'
echo ""
echo "Response:\n"
curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n" http://10.10.0.4:8090/kvs/y

echo "----------------------------------------------"
echo "# DELETE y via 10.0.0.2 w/o causal-metadata"
echo ""
echo "Response:\n"
echo '$curl --request DELETE --header "Content-Type: application/json" --write-out "\n%{http_code}\n" http://10.10.0.2:8090/kvs/y'

sleep 1

echo "----------------------------------------------"
echo "# check for y (should be gone) via 10.10.0.3"
echo '$curl --request GET --header "Content-Type: application/json" --write-out "\\n%{http_code}\\n" http://10.10.0.3:8090/kvs/y'
echo ""
echo "Response:\n"
curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n" http://10.10.0.3:8090/kvs/y

echo "----------------------------------------------"
echo "# DELETE x via 10.0.0.3 with causal-metadata"
echo '$curl --request DELETE --header "Content-Type: application/json" \
--data ''{ "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}'' \
--write-out "\\n%{http_code}\\n" http://10.10.0.3:8090/kvs/x'
echo ""
echo "Response:\n"
curl --request DELETE --header "Content-Type: application/json" \
--data '{ "causal-metadata": {"sender_id": "client", "action": "PUT", "key": "x", "value": 1, "vector_clocks": [{"id": "10.10.0.2:8090", "clock": 1}, {"id": "10.10.0.3:8090", "clock": 0}, {"id": "10.10.0.4:8090", "clock": 0}]}}' \
--write-out "\n%{http_code}\n" http://10.10.0.3:8090/kvs/x

sleep 1

echo "----------------------------------------------"
echo "# check for x (should be gone) via 10.10.0.2"
echo 'curl --request GET --header "Content-Type: application/json" --write-out "\\n%{http_code}\\n" http://10.10.0.2:8090/kvs/x'
echo ""
echo "Response:\n"
curl --request GET --header "Content-Type: application/json" --write-out "\n%{http_code}\n" http://10.10.0.2:8090/kvs/x
