from flask import Flask, request, jsonify
from admin import admin
from internal import internal
from data_key import client_side
from external_key import external_key
from kvs import get_all
from sync import sync_2
import os
import globals
from flask_apscheduler import APScheduler

def is_valid_ipv4_address(ip):
    parts = ip.split('.')
    if len(parts) != 4:
        return False
    for part in parts:
        if not part.isdigit() or int(part) > 255 or int(part) < 0:
            return False
    return True



app = Flask(__name__)
scheduler = APScheduler()
track_var = 0

try:
  globals.address = os.environ['ADDRESS']
  if is_valid_ipv4_address(globals.address.split(":")[0]) == False:
    track_var = 1
  if not globals.address.split(":")[1]:
    track_var = 1
except:
  track_var = 1


app.register_blueprint(admin, url_prefix="/kvs/admin")
app.register_blueprint(internal, url_prefix="/kvs/internal")
app.register_blueprint(client_side, url_prefix="/internal")
app.register_blueprint(external_key, url_prefix="/kvs/data")
app.register_blueprint(get_all)

if __name__ == "__main__":
    if track_var == 1:
      exit(1)
    scheduler.add_job(id = 'Scheduled Task', func=sync_2, trigger="interval", seconds=3)
    scheduler.start()
    app.run(host='0.0.0.0', port=8080)


