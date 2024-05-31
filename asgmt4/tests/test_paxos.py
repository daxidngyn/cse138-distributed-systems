import hashlib
from collections import namedtuple
import uuid
import math
import requests
import unittest
from unittest import mock
import paxos
from paxos import Proposer, Acceptor, MessageType
from unittest.mock import patch, Mock

def mocked_send_post(*args, **kwargs):
    print(f"##### args = {args}")
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://alice:8090/shardalloc':
        return MockResponse(args[1], 200)
    elif args[0] == 'http://someotherurl.com/anothertest.json':
        return MockResponse({"key2": "value2"}, 200)

    return MockResponse(None, 404)

class TestPaxos(unittest.TestCase):

    def test_ping(self):
        self.assertTrue(paxos.ping("google.com:80"))
        # fails
        self.assertFalse(paxos.ping("google.comm"))
        self.assertFalse(paxos.ping("google.comm:80"))
        self.assertFalse(paxos.ping("google.com:81"))

    @mock.patch('paxos.ping')
    @mock.patch('requests.post', side_effect=mocked_send_post)
    def atest_send(self, ping_mock, send_mock):
        ping_mock.return_value = True
        results = []
        paxos.send(results, 0, 'alice:8090', {"key1": "value1"})
        self.assertEqual(ping_mock.call_count, 1)
        ping_mock.assert_called_once()
        self.assertIn(mock.call('http://alice:8090/shardalloc'), send_mock.call_args_list)

        self.assertEqual(len(send_mock.call_args_list), 4)
        print(results)

    @patch('requests.post')
    @patch('paxos.ping')
    def test_send_success(self, mock_ping, mock_post):
        mock_ping.return_value = True
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        results = {}
        addr = 'localhost'
        payload = {'key': 'value'}
        
        paxos.send(results, addr, payload)
        
        self.assertEqual(results[addr], mock_response)
        
        # We can even assert that our mocked method was called with the right parameters
        mock_post.assert_called_once_with(f'http://{addr}/shardalloc', json=payload, headers={'Content-type': 'application/json'}, timeout=5)
        # same as calling
        self.assertIn(mock.call(f'http://{addr}/shardalloc', json=payload, headers={'Content-type': 'application/json'}, timeout=5), mock_post.call_args_list)
        # can even check to different parameters that were used in the mock_post
        # As specified in requests.post() function signature,
        # *args (args_list) contain only f'http://{addr}/shardalloc'
        # you can see args in mock_post.call_args.args
        print(f"##### args = {mock_post.call_args.args}")
        self.assertEqual(len(mock_post.call_args.args), 1)
        # **kwargs (keyword args, expanded out), thus kwargs_list, has 3: json=, headers=, and timeout=
        # you can see kwargs in mock_post.call_args.kwargs
        print(f"##### kwargs = {mock_post.call_args.kwargs}")
        self.assertEqual(len(mock_post.call_args.kwargs), 3)
        
    @patch('requests.post')
    @patch('paxos.ping')
    def test_send_failure(self, mock_ping, mock_post):
        mock_ping.return_value = False
        
        results = {}
        addr = 'localhost'
        payload = {'key': 'value'}
        
        paxos.send(results, addr, payload)
        
        self.assertIsNone(results[addr])
        mock_post.assert_not_called()



if __name__ == '__main__':
    try:
        unittest.main(verbosity=0)
    except KeyboardInterrupt:
        TestPaxos.tearDownClass()


# This method will be used by the mock to replace requests.get
'''
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == 'http://someurl.com/test.json':
        return MockResponse({"key1": "value1"}, 200)
    elif args[0] == 'http://someotherurl.com/anothertest.json':
        return MockResponse({"key2": "value2"}, 200)

    return MockResponse(None, 404)
'''

