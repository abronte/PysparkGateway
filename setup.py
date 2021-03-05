#!/usr/bin/env python
import os
from distutils.core import setup

with open('README.rst') as fp:
    readme = fp.read()

setup(
    name='PysparkGateway',
    version='0.0.22',
    packages=[
        'pyspark_gateway',
        'pyspark_gateway.patch_files'],
    license='Apache 2.0',
    description='Connect Pyspark to remote clusters',
    long_description=readme,
    install_requires=[
        'requests>=2.25.1',
        'Flask~=1.1.2',
        'py4j~=0.10.9.2',
        'findspark~=1.4.2'
        ],
    extras_require={
        'dev': [
            'pandas',
            'psutil'
        ]
    },
    python_requires='>=3.6',
    scripts=['bin/pyspark-gateway'],
    url='https://github.com/abronte/PysparkGateway',
    author='Adam Bronte',
    author_email='adam@bronte.me',
)
