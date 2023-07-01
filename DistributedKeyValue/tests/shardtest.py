import string
import sys
import time
import random
import unittest

import requests


def usage():
    print(
        f'Usage: {sys.argv[0]} local_port1:ip1:port1 local_port2:ip2:port2 [local_port3:ip3:port3...]')
    sys.exit(1)


def check_arg_count():
    if len(sys.argv) < 3:
        usage()

def parse_args():
    check_arg_count()
    local_ports = []
    view = []
    for arg in sys.argv[1:]:
        try:
            col1_idx = arg.find(':')
            local_ports.append(int(arg[:col1_idx]))
            view.append(arg[col1_idx+1:])
        except:
            usage()
    return local_ports, view

ports, view_addresses = parse_args()
hosts = ['localhost'] * len(ports)
keys = ['key1', 'key2', 'key3']
vals = ['Value 1', 'val2', 'third_value']
causal_metadata_key = 'causal-metadata'

# Requests:


def get(url, body={}):
    return requests.get(url, json=body)


def put(url, body={}):
    return requests.put(url, json=body)


def delete(url, body={}):
    return requests.delete(url, json=body)


# URLs:


def make_base_url(port, host='localhost', protocol='http'):
    return f'{protocol}://{host}:{port}'


def kvs_view_admin_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/admin/view'


def kvs_data_key_url(key, port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data/{key}'


def kvs_data_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data'

# Bodies:


def nodes_list(ports, hosts=None):
    if hosts is None:
        hosts = ['localhost'] * len(ports)
    return [f'{h}:{p}' for h, p in zip(hosts, ports)]


def put_view_body(addresses, total_shards):
    return {'nodes': addresses, 'num_shards':total_shards}


def causal_metadata_body(cm={}):
    return {causal_metadata_key: cm}


def causal_metadata_from_body(body):
    return body[causal_metadata_key]


def put_val_body(val, cm=None):
    body = causal_metadata_body(cm)
    body['val'] = val
    return body

class TestAssignment(unittest.TestCase):
    #delete every node
    def setUp(self):
        for h, p in zip(hosts,ports):
            delete(kvs_view_admin_url(p,h))
        time.sleep(0.2)

    def test_distribution(self):
        #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(10)

        #check keys are appro
        for h, p in zip(hosts, ports):
            res = get(kvs_data_url(p, h))
            # print(res.json().get('count')/2, 100/2)
            self.assertAlmostEqual(res.json().get('count'), 500/2, delta=50)


    def test_redistribution_add_shards(self):
        #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(7)

        #send a viewchange with a different number of nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, 4))
        
        time.sleep(5)
            
        for h, p in zip(hosts, ports):
            res = get(kvs_data_url(p, h))
            # print(res.json().get('count')/3, 100/3)
            self.assertAlmostEqual(res.json().get('count'), 500/4, delta=20)

    def test_redistribution_remove_shards(self):
        #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(7)

        #send a viewchange with a different number of nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, 1))
        
        time.sleep(5)
            
        for h, p in zip(hosts, ports):
            res = get(kvs_data_url(p, h))
            # print(res.json().get('count')/3, 100/3)
            self.assertAlmostEqual(res.json().get('count'), 500, delta=0)
        
        

    def test_duplicate_keys(self):
         #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(6)

        kvs_hosts = {}
        for (host, port) in zip(hosts, ports):
            json = get(kvs_data_url(port, host)).json()
            shard_id = json.get('shard_id')
            if shard_id not in kvs_hosts:
                kvs_hosts[shard_id] = json.get('keys')
        
        total_keys = []
        for host, kvs in kvs_hosts.items():
            if total_keys == []:
                total_keys = kvs
            else:
                for key in kvs:
                    self.assertNotIn(key, total_keys, key + ' was found in multiple shards!')

    def test_get_key_other_shard(self):
         #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(50):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(6)

        key = get(kvs_data_url(ports[0], hosts[0])).json().get('keys')[0]
        val = get(kvs_data_key_url(key, ports[0], hosts[0])).json().get('val')
        for (host, port) in zip(hosts, ports):
            self.assertEqual(val, get(kvs_data_key_url(key, port, host)).json().get('val'), 'key was not accessible from ' + kvs_data_url(ports[0], hosts[0]))
    
    def test_get_nonexistent_key(self):
         #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)//2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        for _ in range(50):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(6)

        self.assertNotEqual(int(418), get(kvs_data_key_url('a', ports[0], hosts[0])).json().get('key'), 'invalid key was thought that it existed')

    def test_remove_node_shard(self):
        #initialize the view, with ~half as many shards as nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, 2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(0.2)

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(6)

        changed_addresses = view_addresses
        changed_addresses.pop()
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(changed_addresses, 2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(10)

        hosts2 = hosts
        ports2 = ports
        hosts2.pop()
        ports2.remove(int(8082))
        for h, p in zip(hosts2, ports2):
            res = get(kvs_data_url(p, h))
            # print(res.json().get('count')/3, 100/3)
            self.assertAlmostEqual(res.json().get('count'), 500/2, delta=25)

    def test_add_node_shard(self):
        #initialize the view, with ~half as many shards as nodes
        changed_addresses = view_addresses
        add_node = changed_addresses.pop()
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(changed_addresses, 2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(1)

        hosts2 = hosts
        ports2 = ports
        hosts2.pop()
        print(ports2)
        ports2.remove(int(8082))

        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            rand_node = random.choice(range(len(hosts2)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]), 
                put_val_body(v))
            
        time.sleep(6)


        changed_addresses.append(add_node)
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(changed_addresses, 2))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        time.sleep(10)

        for h, p in zip(hosts, ports):
            res = get(kvs_data_url(p, h))
            # print(res.json().get('count')/3, 100/3)
            self.assertAlmostEqual(res.json().get('count'), 500/2, delta=25)

    def test_values_after_shard_change(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, len(hosts)))
        self.assertEqual(res.status_code, 200, msg="Bad status code on PUT view")

        data = {}
        for _ in range(500):
            #generate random key and value pair
            k = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            v = ''.join(random.choice(string.ascii_letters) for _ in range(10))
            data[k] = v
            rand_node = random.choice(range(len(hosts)))
            put(kvs_data_key_url(k, ports[rand_node], hosts[rand_node]),
                put_val_body(v))

        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses, 1))

        time.sleep(10)

        for key, value in data.items():
            rand_node = random.choice(range(len(hosts)))
            resp = get(kvs_data_key_url(key, ports[rand_node], hosts[rand_node]))
            json = resp.json()
            self.assertEqual(value, json.get('val'), msg="val doesn't match stored val")



    
        



if __name__ == '__main__':
    unittest.main(argv=["first-arg-ignored"], exit=False)
