import sys
import os
import atexit
import pkgutil

import requests
from py4j.java_gateway import JavaGateway, GatewayParameters

from pyspark_gateway.server import HTTP_PORT, GATEWAY_PORT

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

        self.patch()
        self.check_version()
        self.start_gateway()

    @classmethod
    def open_tmp_tunnel(cls, port):
        r = requests.post(cls.http_url+'/tmp_tunnel', json={'port': port})
        return r.json()['port']

    def patch(self):
        path = os.path.dirname(os.path.realpath(__file__))+'/patch_files/java_gateway_patch.py'
        patch_file = open(path, 'r').read()

        pkg = pkgutil.get_loader('pyspark')

        path = pkg.get_filename().split('/')[:-1]
        path.append('java_gateway.py')
        path = '/'.join(path)

        if os.path.exists(path+'c'):
            os.remove(path+'c')

        original_file = open(path, 'r').read()

        with open(path, 'w') as f:
            f.write(patch_file)

        def put_back(data, path):
            with open(path, 'w') as f:
                f.write(data)

        atexit.register(put_back, original_file, path)

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
