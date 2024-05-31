import hashlib
from collections import namedtuple
import uuid
import math
from server import Server
import unittest

class TestShard(unittest.TestCase):
    def test_check_minimum_shard_count_true(self):
        kserver = Server("test_dead_nodes")
        shards = { "alligator": [1,2,3,4,5,6], "buffalo": [6, 7]}
        self.assertTrue(kserver.shard._check_minimum_shard_count(shards))

    def test_check_minimum_shard_count_empty(self):
        kserver = Server("test_dead_nodes")
        self.assertFalse(kserver.shard._check_minimum_shard_count({}))

    def test_check_minimum_shard_count_not_enough_members(self):
        kserver = Server("test_dead_nodes")
        shards = { "alligator": [1,2,3,4,5,6], "buffalo": [7]}
        self.assertFalse(kserver.shard._check_minimum_shard_count(shards))

    def test_redistribute_shards(self):
        ''' view has 6 elements '''
        view = ['10.10.0.2:8090', '10.10.0.3:8090', '10.10.0.4:8090', '10.10.0.5:8090', '10.10.0.6:8090', '10.10.0.7:8090']

        kserver = Server("test_redistribute_shards")

        shards = kserver.shard._redistribute_shards(2, view)

        self.assertEqual(len(shards), 2)
        self.assertIn('alligator', shards)
        self.assertIn('buffalo', shards)
        set1 = shards['alligator']
        set2 = shards['buffalo']
        for a in set1:
            self.assertTrue(a not in set2)
        for b in set2:
            self.assertTrue(b not in set1)

        total_nodes = [ x for l in list(shards.values()) for x in l ]
        self.assertEqual(len(total_nodes), 6)

        # check_dups sufficiently guarantees no shards containing dup nodes
        self.check_dups(total_nodes)
        
        for shard_id  in shards:
            self.assertTrue(len(shards[shard_id]) == 3)
        

    def test_redistribute_shards_odd(self):
        ''' view has 5 elements '''
        view = ['10.10.0.2:8090', '10.10.0.3:8090', '10.10.0.4:8090', 
                '10.10.0.5:8090', '10.10.0.6:8090' ]
        shards = { "alligator": [1,2,3,4,5,6], "buffalo": [7,8,9]}

        kserver = Server("test_dead_nodes_increase_shard_count")

        shards = kserver.shard._redistribute_shards(2, view)

        self.assertTrue(type(shards) is dict)
        self.assertEqual(len(shards), 2)
        self.assertIn('alligator', shards)
        self.assertIn('buffalo', shards)

        total_nodes = [ x for l in list(shards.values()) for x in l ]
        self.assertEqual(len(total_nodes), 5)

        # check_dups sufficiently guarantees no shards containing dup nodes
        self.check_dups(total_nodes)
        
        for shard_id in shards:
            self.assertTrue(len(shards[shard_id]) >= 2)

    def test_redistribute_shards_odd2(self):
        ''' view has 7 elements '''
        view = ['10.10.0.9:8090','10.10.0.2:8090','10.10.0.3:8090', '10.10.0.4:8090', '10.10.0.5:8090', 
                '10.10.0.6:8090', '10.10.0.7:8090']

        kserver = Server("test_redistribute_shards_odd2")

        shards = kserver.shard._redistribute_shards(2, view)

        self.assertTrue(type(shards) is dict)
        self.assertEqual(len(shards), 2)
        self.assertIn('alligator', shards)
        self.assertIn('buffalo', shards)

        total_nodes = [ x for l in list(shards.values()) for x in l ]
        self.assertEqual(len(total_nodes), 7)

        # check_dups sufficiently guarantees no shards containing dup nodes
        self.check_dups(total_nodes)
        
        self.assertTrue(len(shards['alligator']) == 4)
        self.assertTrue(len(shards['buffalo']) == 3)
        self.assertTrue('10.10.0.9:8090' not in shards['alligator'])

    def atest_add_nodes(self):
        ''' Add 2 more nodes; shard count remains the same '''
        view = ['10.10.0.2:8090', '10.10.0.3:8090', '10.10.0.4:8090', '10.10.0.5:8090', '10.10.0.6:8090', '10.10.0.7:8090']
        shards = { "alligator": [1,2,3,4,5,6], "buffalo": [7,8,9]}

        kserver = Server("test_dead_nodes_increase_shard_count")

        shards = kserver.shard._redistribute_shards(2, view)
        print(shards)
        self.assertTrue(type(shards) is dict)
        self.assertEqual(len(shards), 2)
        self.assertIn('alligator', shards)
        self.assertIn('buffalo', shards)

        total_nodes = [ x for l in list(shards.values()) for x in l ]
        self.assertEqual(len(total_nodes), 6)

        # check_dups sufficiently guarantees no shards containing dup nodes
        self.check_dups(total_nodes)
        
        # check for even distribution
        for shard_id in shards:
            self.assertTrue(len(shards[shard_id]) == 3)

    def atest_add_nodes_increase_shard_count(self):
        ''' Add 2 more nodes; shard count is increased '''
        view = ['10.10.0.2:8090', '10.10.0.3:8090', '10.10.0.4:8090', '10.10.0.5:8090', '10.10.0.6:8090', '10.10.0.7:8090']
        shards = { "alligator": [1,2,3,4,5,6], "buffalo": [7,8,9]}

        kserver = Server("test_dead_nodes_increase_shard_count")

        shards = kserver.shard._redistribute_shards(4, view)

        self.assertTrue(type(shards) is dict)
        self.assertEqual(len(shards), 4)
        self.assertIn('alligator', shards)
        self.assertIn('buffalo', shards)

        total_nodes = [ x for l in list(shards.values()) for x in l ]
        self.assertEqual(len(total_nodes), 6)

        # check_dups sufficiently guarantees no shards containing dup nodes
        self.check_dups(total_nodes)
        
        # check for even distribution
        for shard_id in shards:
            self.assertTrue(len(shards[shard_id]) >= 2 and len(shards[shard_id]) <= 3) 
    
    def check_dups(self, l: list):
        s = set(l)
        self.assertEqual(len(s), len(l))

if __name__ == '__main__':
    try:
        unittest.main(verbosity=0)
    except KeyboardInterrupt:
        TestShard.tearDownClass()


