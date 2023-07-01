from flask import Flask, request, jsonify, Blueprint
import requests
import globals
from broadcast import broadcast, broadcast_shard
from vector_clocks import *
import asyncio
from datetime import datetime


client_side = Blueprint('client_side', __name__, url_prefix= '/internal')
EIGHT_MEGABYTES = 8388608

@client_side.route('/<key>', methods = ['PUT', 'DELETE'])
def handle_put(key):

  if (len(request.url) > 2048):
     return jsonify(error='URL is too large'), 414

  # get body and data
  body = request.get_json()

  causal_metadata = body.get('causal-metadata', None)
  if causal_metadata is not None:
    update_known_clocks(causal_metadata)
    request_clock = causal_metadata.get(key, None)
  else:
    causal_metadata = dict()
    request_clock = None
  val = body.get('val')

  if request.method == 'PUT' and ('val' not in body.keys() or len(body.keys()) > 2):
     return jsonify({"causal-metadata" : causal_metadata, "error" : "bad request"}), 400
  
  if request.method == 'DELETE' and ('causal-metadata' not in body.keys() and len(body.keys()) > 1):
     return jsonify({"causal-metadata" : causal_metadata, "error" : "bad request"}), 400

  if key is None:
     return jsonify({"causal-metadata" : causal_metadata, "error" : "bad request"}), 400

  if (globals.node_id == -1):
      return jsonify({"causal-metadata" : causal_metadata, 'error' : 'uninitialized'}), 418

  if request.method == 'PUT':
    if len(val) > EIGHT_MEGABYTES:
      return jsonify(error="val too large"), 400
    if val is None:
      return jsonify(error="bad request"), 400

  return_code = 200 if key in globals.local_data else 201

  if key not in globals.local_clocks.keys():
    add_key(globals.local_clocks, key)
  if key not in globals.known_clocks.keys():
    add_key(globals.known_clocks, key)

  # comparing vector clocks ##
  result = compare(globals.local_clocks, key, causal_metadata.get(key, [0] * len(globals.current_view)))
  if result == 0:
    combine(globals.local_data, key, causal_metadata.get(key, []))
  elif result == -1: # if result is -1, ie the client's vector clock is greater than self's
    copy_key(globals.local_clocks, key, causal_metadata[key]) # set self's clock to that of the client

  # update vc

  increment(globals.local_clocks, key, globals.node_id)
  increment(globals.known_clocks, key, globals.node_id)
  # broadcast
  responses = asyncio.run(broadcast_shard(globals.shard_view[globals.shard_member], request.method, f'/kvs/internal/replicate/{key}', key, globals.local_clocks, val=val, source=globals.address, node_id=globals.node_id)) # change data_clocks[key] to the sending_vc

  return jsonify({"causal-metadata" : globals.known_clocks}), return_code

  # return vc and return_code ##
  

@client_side.route("/<key>", methods=["GET"])
def get(key):
    start_time = datetime.now()
    if (len(request.url) > 2048):
     return jsonify(error='URL is too large'), 414

    #get the json object from the request
    json = request.get_json()

    #get the metadata from the json
    temp = json.get('causal-metadata', None)
    if temp is not None:
      causal_metadata = dict(json.get('causal-metadata', None))
      update_known_clocks(causal_metadata)
      request_clock = causal_metadata.get(key, None)
    else:
       causal_metadata = dict()
       request_clock = list()

    if 'causal-metadata' not in json.keys() and len(json.keys()) >= 1:
       return jsonify({"causal-metadata" : causal_metadata, "error" : "bad request"}), 400
    
    if key is None:
       return jsonify({"causal-metadata" : causal_metadata, "error" : "bad request"}), 400

    if (globals.node_id == -1):
      return jsonify({"causal-metadata" : causal_metadata, 'error' : 'uninitialized'}), 418

    #Request clock not existing means message isn't causally dependant on the value 
    if request_clock == None:
        #check if we've seen the key 
        if globals.known_clocks.get(key) == None:
            #if not return an error
            return jsonify({"causal-metadata" : globals.known_clocks}), 404
        elif compare(globals.local_clocks, key, globals.known_clocks.get(key)) == 2:
            if globals.local_data.get(key, None) is None:
              return jsonify({"causal-metadata": globals.known_clocks}), 404
            return jsonify({"val" : globals.local_data[key], "causal-metadata" : globals.known_clocks})
        
    #compare internal clock to response clock
    #keep looping while the metadata is behind
    while(compare(globals.local_clocks, key, causal_metadata.get(key, [0]*len(globals.current_view)))<=0):
        #if internal behind, check with other replica's for updates. 
        #either a response with the newer vector clock, or hang
        responses = asyncio.run(broadcast_shard(globals.shard_view[globals.shard_member], "GET", f"/kvs/internal/replicate/{key}", key, causal_metadata[key], val=None, node_id=globals.node_id, source= globals.address))
        #tmp variable to hold the newst list/val seen
        newest_clock = globals.local_clocks.get(key, [0] * len(globals.current_view))
        newest_value = globals.local_data.get(key)
        last_writer = globals.last_write.get(key)
        #find most updated vector clock, and take it's value
        for r in responses:
            if(r == -1):
                continue
            try:
              json = r.json()
            except:
               continue
            response_clock = json.get('vector_clock')
            if compare(json, 'vector_clock', newest_clock) == -1:
                #if the request clock is behind ours ignore it
                pass
            elif(compare(json, 'vector_clock', newest_clock)==0):
                #if the request clock is concurrent, tie break and combine clocks
                if last_writer > json.get('last-write'):
                    newest_value = json.get('val')
                    last_writer = json.get('last-write')
                for index in range(len(newest_clock)):
                    newest_clock[index] = max(newest_clock[index],request_clock[index])
            elif(compare(json, 'vector_clock', newest_clock) == 1):
                #otherwise the client clock is in the future
                newest_clock = request_clock
                newest_value = json.get('val')
                last_writer = json.get('last-write')

        #update internal information
        globals.local_clocks[key] = newest_clock
        combine(globals.known_clocks, key, newest_clock)
        globals.local_data[key] = newest_value
        globals.last_write[key] = last_writer



        if ((datetime.now() - start_time).total_seconds() >= 20):
           return jsonify({"causal-metadata" : globals.known_clocks, "error" : "timed out while waiting for depended updates"}), 500
           
    #now we know our internal information is synced at least to where the client was, 
    #so everything is causally consistent. 


    #and return the data
    if globals.local_data.get(key, None) is None:
      return jsonify({"causal-metadata": globals.known_clocks}), 404
    return jsonify({"val" : globals.local_data[key], "causal-metadata" : globals.known_clocks})
