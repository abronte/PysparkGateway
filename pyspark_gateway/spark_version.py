import sys

try:
    from pyspark.version import __version__
except:
    import findspark
    findspark.init()

    from pyspark.version import __version__

def spark_version():
    major, minor, patch = __version__.split('.')

    return int(major), int(minor), int(patch)

def valid_spark_version():
    major, minor, patch = spark_version()

    # requires spark >= 2.4
    if major >= 2 and minor >= 4:
        return True
    else:
        return False
