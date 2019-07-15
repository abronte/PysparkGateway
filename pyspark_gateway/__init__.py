import sys
import os
import atexit
import pkgutil

import requests
from py4j.java_gateway import JavaGateway, GatewayParameters

from pyspark_gateway.server import HTTP_PORT, GATEWAY_PORT

# Function to patch from pyspark
#
# License for below function from pyspark
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
def local_connect_and_auth(port, auth_secret):
    from pyspark_gateway import PysparkGateway

    tmp_port = PysparkGateway.open_tmp_tunnel(port)

    """
    Connect to local host, authenticate with it, and return a (sockfile,sock) for that connection.
    Handles IPV4 & IPV6, does some error handling.
    :param port
    :param auth_secret
    :return: a tuple with (sockfile, sock)
    """
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo(PysparkGateway.host, tmp_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(15)
            sock.connect(sa)
            sockfile = sock.makefile("rwb", 65536)
            _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = _exception_message(e)
            errors.append("tried to connect to %s, but an error occured: %s" % (sa, emsg))
            sock.close()
            sock = None
    else:
        raise Exception("could not open socket: %s" % errors)
try:
    from pyspark import java_gateway
except:
    import findspark
    findspark.init()

    from pyspark import java_gateway

java_gateway.local_connect_and_auth = local_connect_and_auth

class PysparkGateway(object):
    host = None
    http_url = None
    gateway = None

    def __init__(self,
            host=os.environ.get('PYSPARK_GATEWAY_HOST', 'localhost'),
            http_port=os.environ.get('PYSPARK_GATEWAY_HTTP_PORT', HTTP_PORT)):

        self.host = host
        self.http_url = 'http://%s:%d' % (host, int(http_port))
        PysparkGateway.http_url = self.http_url
        PysparkGateway.host = self.host

        self.check_version()
        self.start_gateway()

    @classmethod
    def open_tmp_tunnel(cls, port):
        r = requests.post(cls.http_url+'/tmp_tunnel', json={'port': port})
        return r.json()['port']

    def check_version(self):
        from pyspark_gateway.spark_version import spark_version, valid_spark_version
        from pyspark_gateway.version import __version__

        r = requests.get(self.http_url+'/spark_version')
        resp = r.json()

        server_major = resp['spark_major_version']
        server_minor = resp['spark_minor_version']
        server_patch = resp['spark_patch_version']

        client_major, client_minor, client_patch = spark_version()

        if not valid_spark_version():
            print('Pyspark Gateway requires Spark version >= 2.4')

            sys.exit(-1)
        elif 'pyspark_gateway_version' not in resp:
            print('Pyspark Gateway client version doesn\'t match server')
            print('No server version found.')

            sys.exit(-1)
        elif 'pyspark_gateway_version' in resp:
            if __version__ != resp['pyspark_gateway_version']:
                print('Pyspark Gateway client version doesn\'t match server')
                print('Server version: %s' % (resp['pyspark_gateway_version']))
                print('Client version: %s' % (__version__))

                sys.exit(-1)
        elif server_major != client_major or server_minor != client_minor:
            print('Spark server version: %s' % (resp['spark_version']))
            print('Spark client version: %s' % (spark_version))

            sys.exit('Spark version mismatch')

    def start_gateway(self):
        print('Starting local java gateway')

        r = requests.get(self.http_url+'/gateway')
        resp = r.json()

        params = GatewayParameters(
                address=self.host,
                port=GATEWAY_PORT,
                auth_token=resp['auth_token'],
                auto_convert=True)

        self.gateway = JavaGateway(gateway_parameters=params)
