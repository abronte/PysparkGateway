from multiprocessing import Process
import unittest

from py4j.java_gateway import JavaGateway, GatewayParameters
import requests
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext

from pyspark_gateway import server

class PysparkGatewayTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Process(target=server.run, kwargs={'debug': True, 'use_reloader': False})
        cls.server.start()
        cls.server.join(1) # needs some time to boot up the webserver

    @classmethod
    def tearDownClass(self):
        self.server.terminate()

    def test_create_dataframe(self):
        r = requests.get('http://localhost:8765/gateway')
        resp = r.json()

        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=9999, auth_token=resp['auth_token'], auto_convert=True))

        conf = SparkConf().set('spark.io.encryption.enabled', 'true')
        sc = SparkContext(gateway=gateway, conf=conf)

        self.assertEqual(type(sc), SparkContext)

        sc.stop()
        gateway.shutdown()

if __name__ == '__main__':
    unittest.main()
