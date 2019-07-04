#!/usr/bin/env python
import os
from distutils.core import setup

with open('README.rst') as fp:
    readme = fp.read()

setup(
    name='PysparkGateway',
    version='0.0.18',
    packages=[
        'pyspark_gateway',
        'pyspark_gateway.patch_files'],
    license='Apache 2.0',
    description='Connect Pyspark to remote clusters',
    long_description=readme,
    install_requires=[
        'requests>=2.20.0',
        'Flask~=1.0.2',
        'py4j~=0.10.7',
        'findspark~=1.3.0'
        ],
    extras_require={
        'dev': [
            'pandas',
            'psutil'
        ]
    },
    python_requires='>=2.7',
    scripts=['bin/pyspark-gateway'],
    url='https://github.com/abronte/PysparkGateway',
    author='Adam Bronte',
    author_email='adam@bronte.me',
)
