import flask
import time
import atexit
import signal
import os

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

from flask import Flask, request, jsonify
from flask.logging import default_handler

from pyspark_gateway.tunnel import TunnelProcess

HTTP_PORT = 25000
GATEWAY_PORT = 25001
TEMP_PORT = 25002

GATEWAY = None
TMP_PROC = None

app = Flask(__name__)

@app.route('/gateway')
def gateway():
    body = {'auth_token': GATEWAY.gateway_parameters.auth_token}

    return jsonify(body)

@app.route('/tmp_tunnel', methods=['POST'])
def temp_tunnel():
    global TMP_PROC

    if TMP_PROC != None:
        TMP_PROC.proc.terminate()
        TMP_PROC.proc.join(1)
        time.sleep(1)

    req = request.json

    print('Opening temporary tunnel from port %d' % req['port'])

    TMP_PROC = TunnelProcess(TEMP_PORT, req['port'], timeout=5)

    return jsonify({'success': True})

def run(*args, **kwargs):
    global GATEWAY

    from pyspark.java_gateway import launch_gateway

    GATEWAY = launch_gateway()

    p = TunnelProcess(GATEWAY_PORT, GATEWAY.gateway_parameters.port)

    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pyspark gateway web server')

    if 'port' not in kwargs:
        kwargs['port'] = HTTP_PORT

    app.run(*args, **kwargs)

if __name__ == '__main__':
    run(debug=True, use_reloader=False, port=HTTP_PORT)
