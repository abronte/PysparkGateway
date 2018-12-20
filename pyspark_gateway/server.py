import flask
import time

# import findspark
# findspark.init()

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

from pyspark.java_gateway import launch_gateway

from flask import Flask, request, jsonify
from flask.logging import default_handler

from pyspark_gateway.tunnel import TunnelProcess

app = Flask(__name__)

GATEWAY = launch_gateway()

print(GATEWAY.gateway_parameters.port)
print(GATEWAY.gateway_parameters.auth_token)

p = TunnelProcess(9999, GATEWAY.gateway_parameters.port)

@app.route('/gateway')
def gateway():
    body = {
        'auth_token': GATEWAY.gateway_parameters.auth_token,
        'port': GATEWAY.gateway_parameters.port}

    return jsonify(body)

def run(*args, **kwargs):
    if 'debug' not in kwargs or ('debug' in kwargs and kwargs['debug'] == False):
        app.logger.removeHandler(default_handler)
        app.logger = logger

        logger.info('Starting pyspark proxy web server')

    if 'port' not in kwargs:
        kwargs['port'] = 8765

    app.run(*args, **kwargs)

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=8765)
