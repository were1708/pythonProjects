#!/bin/bash



curl -X PUT -d '{"view":["10.10.0.3:8080"]}' -H 'Content-Type: application/json' http://localhost:8081/kvs/admin/view -w '%{http_code}\n'

curl -X GET -d '{}' -H 'Content-Type: application/json' http://localhost:8081/kvs/admin/view -w '%{http_code}\n'

curl -X PUT -d '{"val": "sampleVal", "causal-metadata": {}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/x -w '%{http_code}\n'

curl -X GET -d '{"causal-metadata": {"x" : [1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/x -w '%{http_code}\n'

curl -X PUT -d '{"val": "hello", "causal-metadata": {}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

curl -X GET -d '{"causal-metadata": {"x" : [1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

curl -X PUT -d '{"val": "hello2", "causal-metadata": {"x":[1],"y":[1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

curl -X GET -d '{"causal-metadata": {"x" : [1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

curl -X DELETE -d '{"causal-metadata": {"x" : [1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

curl -X GET -d '{"causal-metadata": {"x" : [1]}}' -H 'Content-Type: application/json' http://localhost:8081/kvs/data/y -w '%{http_code}\n'

