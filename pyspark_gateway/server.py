import flask
import time
import atexit
import signal
import os
import logging

logger = logging.getLogger()

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
    global TMP_PROC, TEMP_PORT

    if TMP_PROC != None:
        TMP_PROC.proc.terminate()
        TMP_PROC.proc.join(1)

    req = request.json

    logger.info('Opening temporary tunnel from port %d to %d' % (req['port'], TEMP_PORT))

    TMP_PROC = TunnelProcess(TEMP_PORT, req['port'])
    TMP_PROC.proc.join(1)

    return jsonify({'port': TEMP_PORT})

@app.route('/spark_version', methods=['GET'])
def spark_version():
    from pyspark_gateway.spark_version import spark_version
    from pyspark_gateway.version import __version__

    major, minor, patch = spark_version()

    resp = {
        'spark_major_version': major,
        'spark_minor_version': minor,
        'spark_patch_version': patch,
        'pyspark_gateway_version': __version__
        }

    return jsonify(resp)

def run(*args, **kwargs):
    global GATEWAY

    if GATEWAY == None:
        from pyspark.java_gateway import launch_gateway

        GATEWAY = launch_gateway()
        TunnelProcess(GATEWAY_PORT, GATEWAY.gateway_parameters.port, keep_alive=True)

    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pyspark gateway server')

    if 'port' not in kwargs:
        kwargs['port'] = HTTP_PORT

    app.run(*args, **kwargs)

if __name__ == '__main__':
    run(debug=True, use_reloader=False, port=HTTP_PORT)
