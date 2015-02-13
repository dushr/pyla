# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import sys


install_requires = [
    'redis==2.7.6',
]

dependency_links = []

setup(
    name='pyla',
    url='http://github.com/dushyant88/pyla',
    version='0.1.0',
    packages=find_packages(),
    install_requires=install_requires,
    dependency_links=dependency_links,
)
