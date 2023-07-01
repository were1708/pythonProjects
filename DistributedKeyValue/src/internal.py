from flask import Flask, request, jsonify, Blueprint
import requests
import globals
from broadcast import broadcast
from vector_clocks import *


internal = Blueprint("internal", __name__, url_prefix="/kvs/internal")

@internal.route('/replicate/<key>', methods = ['GET', 'PUT', 'DELETE'])
def propogate_writes(key):

    body = request.get_json()
    other_clock = body.get('vector_clock')
    val = body.get('val')
    other_id = body.get('id')
    source = body.get('source')
    if source not in globals.current_view:
        return "",403 # node was not in the view!
    
    if request.method == 'GET':
        if key not in globals.local_data.keys():
            return "",404
        return jsonify(val=globals.local_data[key], vector_clock=globals.local_clocks[key], last_write=globals.last_write), 200


    comparison = compare(globals.local_clocks, key, other_clock[key])
    
    if comparison == 2 or comparison == -1: 
        # we can actually do the fucking operation!
        if request.method == 'PUT':
            if comparison == -1:
                copy_key(globals.local_clocks, key, other_clock[key]) # copy the new key into ours!
                combine(globals.known_clocks, key, globals.local_clocks.get(key, [0] * len(globals.current_view)))
            
            if key not in globals.local_data.keys():
                returnVal = 201
            else:
                returnVal = 200
            globals.local_data[key] = val # set the actual value
            globals.last_write[key] = other_id
            return "", returnVal

        if request.method == 'DELETE':
            if comparison == -1:
               copy_key(globals.local_clocks, key, other_clock[key]) # copy the new key into ours!
               combine(globals.known_clocks, key, globals.local_clocks.get(key, [0] * len(globals.current_view)))

            if key in globals.local_data.keys():
                globals.local_data[key] = None
                globals.last_write[key] = other_id
                return "", 200
            globals.last_write[key] = other_id # ask Ronan about this!
            return "", 404

    if comparison == 0 or comparison == 1:
        if comparison == 1: # we're in the future
            return jsonify(vector_clock=globals.local_clocks[key],val=val), 200 # don't replicate a write from the past
            # really, this shouldn't be able to happen tho
        if comparison == 0: # we're concurrent
            # do tie break:
            if request.method == 'PUT':
                if globals.last_write[key] < other_id: # the vaue we have right now wins!
                    combine(globals.local_clocks, key, other_clock.get(key, len(globals.current_view)))
                    combine(globals.known_clocks, key, globals.local_clocks.get(key, [0] * len(globals.current_view)))
                    return"", 200
                else: # we're gonna do the put
                    if key not in globals.local_data.keys():
                        returnVal = 201
                    else:
                        returnVal = 200
                    globals.local_data[key] = val # set the actual value
                    combine(globals.local_clocks, key, other_clock.get(key, len(globals.current_view)))
                    combine(globals.known_clocks, key, globals.local_clocks.get(key, [0] * len(globals.current_view)))
                    globals.last_write[key] = other_id
                    return "", returnVal
            else: # it is a delete!
                if globals.last_write[key] < other_id:
                    return "", 200
                else:
                     if key in globals.local_data.keys():
                        globals.local_data[key] = None
                        combine(globals.local_clocks, key, other_clock.get(key, len(globals.current_view)))
                        combine(globals.known_clocks, key, globals.local_clocks.get(key, [0] * len(globals.current_view)))
                        globals.last_write[key] = other_id
                        return "", 200
                     else:
                        globals.last_write[key] = other_id # ask Ronan about this!
                        return "", 404


@internal.route('/kvs', methods=['GET'])
def get_all():

    body = request.get_json()
    other_id = body.get('id')
    source = body.get('source')

    if source not in globals.current_view:
        return "",403 # node was not in the view!

    other_clock = dict(body.get('vector_clock'))
    return jsonify(vector_clock= globals.local_clocks, kvs= globals.local_data, last_write= globals.last_write), 200

@internal.route('/sync', methods=['PUT'])
def sync_kvs_local_clocks():
    json = request.get_json()
    got_clocks = dict(json.get('vector_clock'))
    got_data = dict(json.get('key'))
    got_last_write = dict(json.get('id'))
    all_keys = set().union(globals.local_data.keys(), got_data.keys())
    for key in all_keys:
        if compare(globals.local_clocks, key, got_clocks.get(key, None)) == -1:
            globals.local_data[key] = got_data.get(key, None)
            globals.local_clocks[key] = got_clocks.get(key, None)
            globals.last_write[key] = got_last_write.get(key, None)
            combine(globals.known_clocks, key, got_clocks.get(key))
        elif compare(globals.local_clocks, key, got_clocks.get(key, None)) == 0 and globals.last_write.get(key, None) > got_last_write.get(key, None):
            globals.local_data[key] = got_data.get(key, None)
            combine(globals.local_clocks, key, got_clocks.get(key, None))
            globals.last_write[key] = got_last_write.get(key, None)
            combine(globals.known_clocks, key, got_clocks.get(key))
        elif compare(globals.local_clocks, key, got_clocks.get(key, None)) == 0 and globals.last_write.get(key, None) <= got_last_write.get(key, None):
            combine(globals.local_clocks, key, got_clocks.get(key, None))
            combine(globals.known_clocks, key, got_clocks.get(key))
        # maybe sanity check if the vector clocks are the same?
    return jsonify(success='updated clocks from sync'), 200
