=====================================
Pyspark Gateway |Build Status| |PyPi|
=====================================

Pypsark Gateway is a library to seamlessly connect to remote spark clusters.

Quick Start
-----------
Install the pysparkgateway package on both the remote Spark cluster you are connecting to and the local machine.

::

   pip install pysparkgateway
   
Start the Pyspark Gateway server on the cluster.

::

   pyspark-gateway start
   
Pyspark Gateway communicates over 3 ports, ``25000``, ``25001``, ``25002``. Currently the client only supports connecting to these ports on localhost so you'll need to tunnel them.


::

   ssh myuser@foo.bar.cluster.com -L 25000:localhost:25000 -L 25001:localhost:25001 -L 25002:localhost:25002

Now you're ready to connect. The main thing to keep in mind is the Pyspark Gateway import **needs to come before any other import.** Pypsark Gateway needs to patch your local pyspark in order to function properly.

The way that your local Python connects to the remote cluster is via a custom py4j gateway. Pyspark Gateway will create and configure automatically, you just need to pass it into the ``SparkContext`` options.

Also to enable all pyspark functions to work, ``spark.io.encryption.enabled`` needs to be set to ``true``.

::

   # This import comes first!
   from pyspark_gateway import PysparkGateway
   pg = PysparkGateway()
   
   from pyspark import SparkContext, SparkConf
   
   conf = conf.set('spark.io.encryption.enabled', 'true')
   sc = SparkContext(gateway=pg.gateway, conf=conf)

Now you have a working spark context connected to a remote cluster.

.. |Build Status| image:: https://travis-ci.org/abronte/PysparkGateway.svg?branch=master
   :target: https://travis-ci.org/abronte/PysparkGateway

.. |PyPi| image:: https://img.shields.io/pypi/v/pysparkgateway.svg
   :target: https://pypi.org/project/PysparkGateway/
