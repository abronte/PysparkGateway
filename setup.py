#!/usr/bin/env python
import os
from distutils.core import setup

with open('README.rst') as fp:
    readme = fp.read()

with open('requirements.txt') as fp:
    requires = fp.read().split('\n')

setup(
    name='PysparkGateway',
    version='0.0.3',
    packages=[
        'pyspark_gateway',
        'pyspark_gateway.patch_files'],
    license='Apache 2.0',
    description='Connect Pyspark to remote clusters',
    long_description=readme,
    install_requires=requires,
    extras_require={
        'dev': [
            'pandas',
            'psutil'
        ]
    },
    python_requires='>=2.7',
    url='https://github.com/abronte/PysparkGateway',
    author='Adam Bronte',
    author_email='adam@bronte.me',
)
