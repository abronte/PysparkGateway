import sys

try:
    from pyspark.version import __version__
except:
    import findspark
    findspark.init()

    from pyspark.version import __version__

def spark_version():
    v = __version__.split('.')
    major = v[0]
    minor = v[1]

    return int(major), int(minor)

def valid_spark_version():
    major, minor = spark_version()

    # requires spark >= 2.4
    if (major >= 2 and minor >= 4) or major >= 3:
        return True
    else:
        return False
