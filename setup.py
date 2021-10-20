# rs-midpoint is available under the MIT License. https://github.com/roundservices/rs-midpoint/
# Copyright (c) 2021, Round Services LLC - https://roundservices.biz/
#
# Author: Gustavo J Gallardo - ggallard@roundservices.biz
#

from setuptools import setup

setup(
    name='rs-couchbase',
    version='1.0.0',
    description='Python utilities for Couchbase',
    url='git@github.com:RoundServices/rs-couchbase.git',
    author='Round Services',
    author_email='ggallard@roundservices.biz',
    license='MIT License',
    install_requires=['testresources', 'couchbase', 'rs-utils'],
    packages=['rs.couchbase'],
    zip_safe=False,
    python_requires='>=3.0'
)
