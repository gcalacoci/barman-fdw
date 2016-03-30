#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'barman_fdw',
    'author': 'Giulio Calacoci',
    'author_email': 'giulio.calacoci@2ndquadrant.it',
    'version': '0.1a1',
    'packages': ['barman_fdw'],
}

setup(**config)
