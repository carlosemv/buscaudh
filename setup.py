#!/usr/bin/env python

from setuptools import setup

setup(name='buscaudh',
	version='0.3',
	packages=['buscaudh'],
	scripts=['buscaudh/bin/buscaudh_script.py'],
	package_data={'buscaudh': ['data/*']},
)