#!/usr/bin/env python

from setuptools import setup

setup (
    setup_requires=['pbr', 'mock'],
    pbr=True,
    package_dir={ 'cassandra_range_repair':'src' },
    packages=['cassandra_range_repair' ],
    test_suite='tests',
)
