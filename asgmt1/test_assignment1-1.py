####################
# Course: CSE138
# Date: Winter 2024
# Assignment: 1
# This document is the copyrighted intellectual property of the authors.
# Do not copy or distribute in any form without explicit permission.
###################

import unittest
import subprocess
import requests

PORT = 8085


class TestHW1(unittest.TestCase):
    # Make some sort of http request
    def test1(self):
        res = requests.get("http://localhost:" + str(PORT) + "/ping")
        self.assertEqual(
            dict(res.json()),
            {"message": "I'm alive!!"},
            msg="Incorrect response to /ping endpoint",
        )

    # Send a parameter with request to app, and access that parameter in app
    def test2(self):
        res = requests.put("http://localhost:" + str(PORT) + "/ping/Slug")
        self.assertEqual(
            dict(res.json()),
            {"message": "I'm alive, Slug!!"},
            msg="Incorrect response to PUT request to /ping/<name> endpoint",
        )

    def test3(self):
        res = requests.get("http://localhost:" + str(PORT) + "/echo")
        self.assertEqual(
            dict(res.json()),
            {"message": "Get Message Received"},
            msg="Incorrect response to GET request to /echo endpoint",
        )
        res = requests.put(
            "http://localhost:" + str(PORT) + "/echo", json={"message": "foo"}
        )
        self.assertEqual(
            dict(res.json()),
            {"message": "foo"},
            msg="Incorrect response to PUT request to /echo endpoint",
        )
        res = requests.put(
            "http://localhost:" + str(PORT) + "/echo",
            json={"message": {"complex object": ["with", "nested", "values"]}},
        )
        self.assertEqual(
            dict(res.json()),
            {"message": {"complex object": ["with", "nested", "values"]}},
            msg="Incorrect response to PUT request to /echo endpoint",
        )

    # Set the status codes of responses
    def test4(self):
        res = requests.get("http://localhost:" + str(PORT) + "/ping")
        self.assertEqual(
            res.status_code,
            200,
            msg="Did not return status 200 to GET request to /ping endpoint",
        )

        res = requests.put("http://localhost:" + str(PORT) + "/ping")
        self.assertEqual(
            res.status_code,
            405,
            msg="Did not return status 405 to PUT request to /ping endpoint",
        )

        res = requests.put("http://localhost:" + str(PORT) + "/ping/Slug")
        self.assertEqual(
            res.status_code,
            200,
            msg="Did not return status 200 to PUT request to /ping/<name> endpoint",
        )

        res = requests.get("http://localhost:" + str(PORT) + "/ping/Slug")
        self.assertEqual(
            res.status_code,
            405,
            msg="Did not return status 405 to GET request to /ping/<name> endpoint",
        )

        res = requests.put(
            "http://localhost:" + str(PORT) + "/echo", json={"message": "foo"}
        )
        self.assertEqual(
            res.status_code,
            200,
            msg="Did not return status 200 to PUT request to /echo endpoint with valid body",
        )

        res = requests.get("http://localhost:" + str(PORT) + "/echo")
        self.assertEqual(
            res.status_code,
            200,
            msg="Did not return status 200 to GET request to /echo endpoint",
        )

        res = requests.put("http://localhost:" + str(PORT) + "/echo")
        self.assertEqual(
            res.status_code,
            400,
            msg="Did not return status 400 to PUT request to /echo endpoint when no message was given",
        )

        res = requests.put(
            "http://localhost:" + str(PORT) + "/echo", json={"not_message": "foo"}
        )
        self.assertEqual(
            res.status_code,
            400,
            msg="Did not return status 400 to PUT request to /echo endpoint when invalid body was given",
        )


if __name__ == "__main__":
    unittest.main()
