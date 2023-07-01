import globals
from flask import request, Response
import asyncio
import requests
from multiprocessing import Pool

async def async_request(ip, path, method, key = None, vector_clock = dict(), val = None, node_id = globals.node_id, source = globals.address):
  url = f"http://{ip}{path}"
  # path needs to include the /<key> if that endpoint needs one
  state = {"key": key, "val": val, "vector_clock" : vector_clock, "id" : node_id, "source" : source}
  # print(state)
  request = []
  try:
    if method == 'PUT':
      request = requests.put(url, json=state, timeout=(2))
    elif method == 'GET':
      request = requests.get(url, json=state, timeout=(2))
    else:
      request = requests.delete(url, json=state, timeout=(2))
  except:
    return -1
  
  return request

# Input:
#   method: the method on the end point in string form
#   path: the path of the end point with the key (ie /internal/write/)
#   key: the key in the kvs to modify/get
#   vector_clock: the vector_clock of the node
#
# Output:
#   responses: a list of responses given from all the requests
async def broadcast(method, path, key, vector_clock, val=None, node_id = globals.node_id, source = globals.address):
  tasks = []
  for node in globals.current_view:
    task = asyncio.create_task(async_request(ip = node, path = path, method = method, key = key, vector_clock= vector_clock, val = val, node_id= node_id, source = source))
    tasks.append(task)
  responses = await asyncio.gather(*tasks)
  return responses

async def broadcast_shard(shard, method, path, key, vector_clock, val=None, node_id = globals.node_id, source = globals.address):
  tasks = []
  for node in shard:
    task = asyncio.create_task(async_request(ip = node, path = path, method = method, key = key, vector_clock= vector_clock, val = val, node_id= node_id, source = source))
    tasks.append(task)
  responses = await asyncio.gather(*tasks)
  return responses


async def proxy_request(address, req, route):
  try:
    response = requests.request(
      method = req.method,
      url = f"http://{address}{route}",
      data = req.data,
      headers = req.headers,
      timeout = 15
          )
    return Response(response.content, response.status_code, response.headers.items())
  except:
    return -1


async def broadcast_request(addresses, req, route):
    tasks = []
    for address in addresses:
      task = asyncio.create_task(proxy_request(address=address, req=req, route=route))
      tasks.append(task)
    responses = await asyncio.gather(*tasks)
    return responses
