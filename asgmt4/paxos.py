import uuid
from flask import Flask, request, jsonify
import requests
import socket
import logging
import math
import json
from enum import Enum

from collections import namedtuple

'''
Based on https://github.com/cocagne/paxos/
'''

def ping(addr):
    ''' Return:
        - True: the only meaningful value
        - False: anything bad, including input, timeout, exceptions
    '''
    host = addr.split(':')
    if len(host) < 2:
        return False
    ret = False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        try:
            connection = sock.connect_ex((host[0], int(host[1])))
            if not connection:
                ret = True
        except Exception as e:
            print(f"ping error: {e}")
    return ret

def broadcast(addresses: list[str], payload, retries=1):
    '''
    Broadcasts a message with payload to all in addresses
    '''
    results = {}
    count = 1
    done = []
    while count <= retries:
        to_do = [ x for x in addresses if x not in done ]
        for i in range(len(to_do)):
            addr = addresses[i]
            send(results, addr, payload)
            if "timed out" != results[addr]:
                done.append(addr)
            if len(done) == len(addresses):
                break
            count += count
    for addr in results:
        if "timed out" == results[addr]:
            results[addr] = None
    return results

def send(results, addr, payload):
    '''
        Sends the payload to addr via POST http://<addr>/shardalloc
        Parameters:
        - results: a non-null array to store responses
        - addr: string of IP_ADDRESS:PORT to send the payload
        - payload: must be JSON string
        Return:
        - no return value. The responses are stored in results as
          results[addr] = response
          This is so that send() can be used in threads.
    '''
    headers = {'Content-type': 'application/json'}
    if ping(addr):  # check destination is alive
        #logging.debug(f"[send] sending to {addr}: {json.dumps(payload)}")
        try:
            resp = requests.post(f'http://{addr}/shard-alloc', json=json.dumps(payload), headers=headers, timeout=1)
            results[addr] = resp
            #logging.info(f"[send] recevied resp: {resp.status_code}, {resp.json()}")
        except requests.exceptions.Timeout as e:
            logging.info(f"[send] Attempt timed out for http://{addr}/shard-alloc")
            results[addr] = "timed out"
        except Exception as e:
            results[addr] = None
            logging.warning(f"[send] got error: {e}")
    else:
        results[addr] = None

  
class Proposer:
    ''' Capturing the initiator. Each time a node wants to be a proposer,
        it instantiates this object and starts it.
    '''
    def __init__(self, addr, view):
        self.view = view
        self.proposer_uid = addr

        # Things that change for each Proposal
        self.proposal_number = 1
        self.current_proposal = None

    def prepare(self) -> set[str]: 
        '''
        Sends a prepare request to all Acceptors as the first step in attempting to
        acquire leadership of the Paxos instance. 

        Return:
        - list of addresses of the nodes that sent back a Promise
        '''
        success_promises = [] 
        self.proposal_number += 1
        self.current_proposal = {"sender_id": self.proposer_uid, "number": self.proposal_number }
        destinations = [ addr for addr in self.view.get()[0].json['view'] if addr != self.proposer_uid ]
        results = self._send_prepare(destinations, self.current_proposal)
        other_proposals = []
        for acceptor_id, status, received_proposal in results:
            addr = self.on_promised(acceptor_id, status, received_proposal)
            if addr:
                success_promises.append(addr)
            else:
                other_proposals.append(received_proposal)

        return (self.current_proposal, success_promises, other_proposals)


    def _send_prepare(self, addresses: list[str], proposal: dict):
        '''
            Broadcasts a Prepare message to all Acceptors
        '''
        payload = { "type": "PREPARE", "proposal": proposal }
        results = broadcast(addresses, payload, 3)
        ret = []
        for sender_addr in results:
            resp = results[sender_addr]
            if resp:
                acceptor_id = resp.json()["auid"]
                status = resp.json()["status"]
                proposal = resp.json()["proposal"]
                proposer_id = proposal["sender_id"]
                proposal_number = proposal["number"]
                ret.append((acceptor_id, status, {"sender_id": proposer_id, "number": proposal_number}))
        
        return ret

    def on_promised(self, acceptor_id, status, received_proposal: dict) -> str:
        '''
            On receiving a promise from an Acceptor
        '''
        # Ignore the message if it's for an old proposal or we have already received
        # a response from this Acceptor
        if status == "rejected" or received_proposal["sender_id"] != self.proposer_uid:
            return None
        
        return acceptor_id

    def send_accept(self, addresses, proposal: dict, proposed_value):
        '''
            Broadcasts an Accept message to all Acceptors
        '''
        payload = { "type": "ACCEPT", 
                    "proposal": proposal, 
                    "proposed_value": proposed_value 
                  }
        print(f"Proposer {self.proposer_uid} sends Accept: {payload}")
        return broadcast(addresses, payload=payload)

        
class Acceptor (object):
    ''' Acceptor of a proposal.
        Phase 1:
        It always replies (to be nice) with a message.
        - If it accepts a proposal, it replies with that proposal with status = "accepted".
        - If it rejects the proposal, it replies with its previously promised proposal
          and status="rejected".
        Thus, the proposer can always look at the status to find out if his proposal
        was accepted (and this Acceptor enters a contract to honor)
        Phase 2:
        If the message Accept is from the current promised proposer and the proposal number
        is the current promised proposal number, it'll broadcast this Accepted() message to
        all Learners.
        If not, it only replies to sender { "error": "not accepted" }
    '''
    def __init__(self, addr, view):
        self.view = view
        self.auid = addr
        self.promised_proposal : dict = None
        self.accepted_proposal = None
        self.accepted_value = None
    
    def get_addresses(self):
        # don't send to oneself
        return [ x for x in self.view.get()[0].json['view'] if x != self.auid ]

    def on_prepare(self, message: dict) -> dict:
        ''' When receiving a Prepare message
            Return:
            - a dict obj that client of this method should convert to
              JSON before replying to an HTTP request
        '''
        proposal = message["proposal"]
        ret = self._recv_prepare(proposal)
        #print(f"Acceptor {self.auid} responds to {proposal}: {ret}")
        return ret

    def _recv_prepare(self, proposal: dict):
        '''
        Called when a Prepare message is received from a Proposer
        '''

        status = "accepted"
        if self.promised_proposal:
            if proposal["number"] > self.promised_proposal["number"]:
                logging.info(f"##### Acceptor {self.auid} accepts {proposal}. Previous self.promised_proposal = {self.promised_proposal}")
                self.promised_proposal = proposal
            elif proposal["number"] == self.promised_proposal["number"] and \
                 proposal["sender_id"] != self.promised_proposal["sender_id"]:
                    status = "rejected"
                    logging.info(f"!!!!! Acceptor {self.auid} REJECTS {proposal} because number <= {self.promised_proposal}")
        else: # nothing yet
            self.promised_proposal = proposal
            logging.info(f"##### Acceptor {self.auid} accepts {proposal}. self.promised_proposal = {self.promised_proposal}")
        
        payload = { 
            "type": "PROMISE",
            "auid": self.auid, 
            "proposal": self.promised_proposal, 
            "status": status
        }
        return payload

    def reject(self, proposal):
        return { 
            "type": "PROMISE",
            "auid": self.auid, 
            "proposal": proposal, 
            "status": "rejected"
        }

    def send_promise(self, proposer_addr, promised_proposal: dict, status):
        '''
        Sends a Promise message to the specified Proposer
        - proposer_addr: to send the message to
        - proposal: the original proposal
        - promised_proposal: the currently promised proposal (can be None)
        - status: 'accepted' or 'rejected'
        
        The payload sent back including:
        - auid: the ID of this Acceptor
        - proposal: the proposal promised on (not necessarily the received proposal)
        - status: "accepted" or "rejected"
        and if Acceptor already delivered a previously Accepted message, will include
        - prev_accepted_proposal: the Proposal that resulted in the accepted value
        - prev_accepted_value: the value delivered from the Accept message associated
          with prev_accepted_proposal
        '''
        
        payload = { 
            "type": "PROMISE",
            "auid": self.auid, 
            "proposal": promised_proposal, 
            "status": status
        }
        # only send back to Proposer
        broadcast([proposer_addr], payload)
                 
    def on_accept(self, proposal: dict, value):
        '''
        Called when an Accept message is received from a Proposer
        '''
        # the Accept message
        payload = {
            "type": "ACCEPTED",
            "proposal": proposal,
            "accepted_value": value
        }
        if proposal["number"] >= self.promised_proposal["number"]:
            self.promised_id     = proposal
            self.accepted_proposal     = proposal
            self.accepted_value  = value
            self.send_accepted(payload)

        # this is for Proposer
        return payload

    def send_accepted(self, payload):
        '''
            Broadcasts an Accepted message to all learners (except self)
        '''
        addresses = [ x for x in self.get_addresses() ]
        broadcast(addresses, payload)

