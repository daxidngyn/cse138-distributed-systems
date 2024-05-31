################### 
# Course: CSE138
# Date: Winter 2024
# Assignment: 2
# This document is the copyrighted intellectual property of the authors.
# Do not copy or distribute in any form without explicit permission.
###################

import unittest
import subprocess
import requests
import sys
import time


'''
This is the test suite for Part 2.

This script will set up three containers and run a set of tests on them.

Do not run your server or any containers manually!
'''


class Part2Const:
    subnetName = "asg2net"
    subnetAddress = "10.10.0.0/16"

    hostname = 'localhost' # Windows and Mac users can change this to the docker vm ip

    main_host_port = "8082"
    main_name = "main-instance"
    main_subnet_ip = "10.10.0.2"

    forwarding1_host_port = "8083"
    forwarding1_name = "forwarding-instance1"
    forwarding1_subnet_ip = "10.10.0.3"

    forwarding2_host_port = "8084"
    forwarding2_name = "forwarding-instance2"
    forwarding2_subnet_ip = "10.10.0.4"


# docker linux commands
#
# * all wait for subprocess to complete
# * all but `docker build` suppress stdout
# * all but the removal commands require success
# * the removal commands require success by default but have an argument which can be set to False to ignore failure
# * the removal commands additionally suppress stderr

def removeSubnet(subnetName, required=True):
    command = "docker network rm " + subnetName
    subprocess.run(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=required)

def createSubnet(subnetAddress, subnetName):
    command  = "docker network create --subnet=" + subnetAddress + " " + subnetName
    subprocess.check_call(command, stdout=subprocess.DEVNULL, shell=True)

def buildDockerImage():
    command = "docker build -t asg2img ."
    subprocess.check_call(command, shell=True)

def runMainInstance(hostPortNumber, ipAddress, subnetName, instanceName):
    command = "docker run -d -p " + hostPortNumber + ":8090 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " asg2img"
    subprocess.check_call(command, shell=True, stdout=subprocess.DEVNULL)

def runForwardingInstance(hostPortNumber, ipAddress, subnetName, instanceName, forwardingAddress):
    command = "docker run -d -p " + hostPortNumber + ":8090 --net=" + subnetName  + " --ip=" + ipAddress + " --name=" + instanceName + " -e FORWARDING_ADDRESS=" + forwardingAddress + " asg2img"
    subprocess.check_call(command, shell=True, stdout=subprocess.DEVNULL)

def stopAndRemoveInstance(instanceName, required=True):
    stopCommand = "docker stop " + instanceName
    subprocess.run(stopCommand, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=required)
    time.sleep(1)
    removeCommand = "docker rm " + instanceName
    subprocess.run(removeCommand, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=required)


class Part2(unittest.TestCase):

    ######################## Functions to send the required requests ##########################################
    def send_all_requests(self, baseUrl):
        # get nonexistent key
        response = requests.get(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

        # delete nonexistent key
        response = requests.delete(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

        # put nonexistent key
        response = requests.put(baseUrl + '/kvs/distsys', json={'value': 138})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'created')

        # get after putting nonexistent key
        response = requests.get(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], 138)

        # put existent key
        response = requests.put(baseUrl + '/kvs/distsys', json={'value': "partial failure"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'replaced')

        # get after putting existent key
        response = requests.get(baseUrl + '/kvs/distsys')
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], "partial failure")

        # delete existent key
        response = requests.delete(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'deleted')

        # get after deleting key
        response = requests.get(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

        # put key with no value
        response = requests.put(baseUrl + '/kvs/distsys', json={})
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'PUT request does not specify a value')

    def send_forwarding12_requests(self, baseUrl1, baseUrl2):
        # put nonexistent key
        response = requests.put(baseUrl1 + '/kvs/distsys', json={'value': 138})
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'created')

        # get after putting nonexistent key
        response = requests.get(baseUrl2 + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], 138)

        # put existent key
        response = requests.put(baseUrl2 + '/kvs/distsys', json={'value': "partial failure"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'replaced')

        # get after putting existent key
        response = requests.get(baseUrl1 + '/kvs/distsys')
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], "partial failure")

        # delete existent key
        response = requests.delete(baseUrl1 + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'deleted')

        # get after deleting key
        response = requests.get(baseUrl2 + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

    def send_requests_forwarding_while_main_stopped(self, baseUrl):

        # put nonexistent? key
        response = requests.put(baseUrl + '/kvs/distsys', json={'value': "what will happen?"})
        self.assertEqual(response.status_code, 503)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Cannot forward request')

        # get after putting nonexistent? key
        response = requests.get(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 503)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Cannot forward request')

        # delete existent? key
        response = requests.delete(baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 503)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Cannot forward request')

    ########################## Run tests #######################################################
    def test_a_all_running_request_main(self):
        print("=== Sending requests to main instance..")
        baseUrl = 'http://' + Part2Const.hostname + ':' + Part2Const.main_host_port
        self.send_all_requests(baseUrl)

    def test_b_all_running_request_forwarding1(self):
        print("=== Sending requests the first forwarding instance..")
        baseUrl = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding1_host_port
        self.send_all_requests(baseUrl)

    def test_c_all_running_request_forwarding2(self):
        print("=== Sending requests to the second forwarding instance..")
        baseUrl = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding2_host_port
        self.send_all_requests(baseUrl)

    def test_d_all_running_request_forwarding12(self):
        print("=== Sending requests to both forwarding instances..")
        baseUrl1 = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding1_host_port
        baseUrl2 = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding2_host_port
        self.send_forwarding12_requests(baseUrl1, baseUrl2)

    def test_e_main_stopped_request_forwarding12(self):
        print("=== Destroying just the main instance..")
        stopAndRemoveInstance(Part2Const.main_name)

        print("=== Sending requests to both forwarding instances (while main instance is stopped)..")
        baseUrl1 = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding1_host_port
        baseUrl2 = 'http://' + Part2Const.hostname + ':' + Part2Const.forwarding2_host_port
        self.send_requests_forwarding_while_main_stopped(baseUrl1)
        self.send_requests_forwarding_while_main_stopped(baseUrl2)

        print("=== Running just the main instance..")
        runMainInstance(Part2Const.main_host_port, Part2Const.main_subnet_ip, Part2Const.subnetName, Part2Const.main_name)

    ############ Test suite lifecycle: setUpClass [setUp test tearDown]* tearDownClass #########
    @classmethod
    def setUpClass(cls):
        print('= Cleaning-up containers and subnet, possibly leftover from a previous interrupted run..')
        stopAndRemoveInstance(Part2Const.main_name, required=False)
        stopAndRemoveInstance(Part2Const.forwarding1_name, required=False)
        stopAndRemoveInstance(Part2Const.forwarding2_name, required=False)
        # Prevent "Error response from daemon: error while removing network: network assignment2-net id ... has active endpoints"
        time.sleep(5)
        removeSubnet(Part2Const.subnetName, required=False)
        print('= Creating image and subnet..')
        buildDockerImage()
        createSubnet(Part2Const.subnetAddress, Part2Const.subnetName)

    def setUp(self):
        print("\n== Running containers..")
        runMainInstance(Part2Const.main_host_port, Part2Const.main_subnet_ip, Part2Const.subnetName, Part2Const.main_name)
        runForwardingInstance(Part2Const.forwarding1_host_port, Part2Const.forwarding1_subnet_ip, Part2Const.subnetName, Part2Const.forwarding1_name, Part2Const.main_subnet_ip + ":8090" )
        runForwardingInstance(Part2Const.forwarding2_host_port, Part2Const.forwarding2_subnet_ip, Part2Const.subnetName, Part2Const.forwarding2_name, Part2Const.main_subnet_ip + ":8090" )
        # Ensure that processes in the containers have time to start up and listen on ports
        time.sleep(5)

    def tearDown(self):
        print("== Destroying containers..")
        stopAndRemoveInstance(Part2Const.main_name)
        stopAndRemoveInstance(Part2Const.forwarding1_name)
        stopAndRemoveInstance(Part2Const.forwarding2_name)

    @classmethod
    def tearDownClass(cls):
        print('= Destroying subnet..')
        removeSubnet(Part2Const.subnetName)


if __name__ == '__main__':
    unittest.main(verbosity=2)

    ##### For debugging the tests for Part2, comment out the above unittest.main(..) and just run:
    # Part2.setUpClass()
    # Part2.setUp(Part2)
