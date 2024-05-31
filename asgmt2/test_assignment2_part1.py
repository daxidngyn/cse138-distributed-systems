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
This is the test suite for Part 1.

Before running this, manually run your server (or container) on host port 8090.
'''


class Part1Const:
    hostname = 'localhost'  # Windows and Mac users can change this to the docker vm ip
    portNumber = '8090'
    baseUrl = 'http://' + hostname + ":" + portNumber


class Part1(unittest.TestCase):

    def test_a_get_absent_key(self):
        response = requests.get(Part1Const.baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

    def test_b_delete_absent_key(self):
        response = requests.delete(Part1Const.baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

    def test_c_put_absent_key(self):
        response = requests.put(Part1Const.baseUrl + '/kvs/distsys', json=dict(value=138))
        self.assertEqual(response.status_code, 201)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'created')

    def test_d_get_after_put_absent_key(self):
        response = requests.get(Part1Const.baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], 138)

    def test_e_put_present_key(self):
        response = requests.put(Part1Const.baseUrl + '/kvs/distsys', json=dict(value="partial failure"))
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'replaced')

    def test_f_get_after_put_present_key(self):
        response = requests.get(Part1Const.baseUrl + '/kvs/distsys')
        self.assertIn('result', response.json())
        self.assertIn('value', response.json())
        self.assertEqual(response.json()['result'], 'found')
        self.assertEqual(response.json()['value'], "partial failure")

    def test_g_delete_present_key(self):
        response = requests.delete(Part1Const.baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 200)
        self.assertIn('result', response.json())
        self.assertEqual(response.json()['result'], 'deleted')

    def test_h_get_after_delete_present_key(self):
        response = requests.get(Part1Const.baseUrl + '/kvs/distsys')
        self.assertEqual(response.status_code, 404)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key does not exist')

    def test_i_put_key_too_long(self):
        tooLongKey = '6TLxbmwMTN4hX7L0QX5_NflWH0QKfrTlzcuM5PUQHS52___lCizKbEMxLZHhtfww3KcMoboDLjB6mw_wFfEz5v_TtHqvGOZnk4_8aqHga79BaHXzpU9_IRbdjYdQutAU0HEuji6Ny1Ol_MSaBF4JdT0aiG_N7xAkoPH3AlmVqDN45KDGBz7_YHrLnbLEK11SQxZcKXbFomh9JpH_sbqXIaifqOy4g06Ab0q3WkNfVzx7H0hGhNlkINf5PF12'
        response = requests.put(Part1Const.baseUrl + '/kvs/' + tooLongKey, json=dict(value="haha"))
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'Key is too long')

    def test_j_put_key_with_no_value(self):
        response = requests.put(Part1Const.baseUrl + '/kvs/oops', json=dict())
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())
        self.assertEqual(response.json()['error'], 'PUT request does not specify a value')


if __name__ == '__main__':
    unittest.main(verbosity=2)
