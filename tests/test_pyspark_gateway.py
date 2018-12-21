import unittest
import time
from multiprocessing import Process
import os

import psutil
import requests
import pandas

from pyspark_gateway import server
from pyspark_gateway import PysparkGateway

class PysparkGatewayTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Process(target=server.run, kwargs={'debug': True, 'use_reloader': False})
        cls.server.start()
        cls.server.join(10) # needs some time to boot up the webserver

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()

        # just for testing so we don't have any
        # zombie procs because we're forking off forks
        # this isn't needed when run on its own
        cur_pid = os.getpid()

        for p in psutil.process_iter():
            if p.name() == 'python' and p.pid != cur_pid:
                p.terminate()

    def test_pyspark_gateway(self):
        pg = PysparkGateway()

        import pyspark
        from pyspark import SparkContext, SparkConf
        from pyspark.sql.context import SQLContext
        from pyspark.sql.functions import udf

        conf = SparkConf().set('spark.io.encryption.enabled', 'true')
        sc = SparkContext(gateway=pg.gateway, conf=conf)
        sqlContext = SQLContext.getOrCreate(sc)

        self.assertEqual(type(sc), SparkContext)

        df = sqlContext.createDataFrame([(1,2,'value 1')], ['id1', 'id2', 'val'])
        self.assertEqual(df.count(), 1)

        rows = df.collect()
        self.assertEqual(rows[0].id1, 1)

        pd = df.toPandas()
        self.assertEqual(type(pd), pandas.core.frame.DataFrame)

        data = [(1,2,'a'),(3,4,'b'),(5,6,'c')]
        df = sqlContext.createDataFrame(data, ['foo', 'bar', 'baz'])
        df.createOrReplaceTempView('foo_table')

        def squared(v):
            return v * v

        sqlContext.udf.register('squared', squared)

        squared_df = sqlContext.sql('select squared(foo) AS val from foo_table')
        rows = squared_df.collect()

        self.assertEqual(rows[2].val, '25')

        sc.stop()
        pg.gateway.shutdown()

if __name__ == '__main__':
    unittest.main()
