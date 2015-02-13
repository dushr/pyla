# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    'redis==2.7.6',
]

dependency_links = []

setup(
    name='pyla',
    url='http://github.com/dushyant88/pyla',
    version='0.1.0b0',
    description="pyla is a redis based storage system",
    long_description=long_description,
    packages=find_packages(),
    install_requires=install_requires,
    dependency_links=dependency_links,
    author="Dushyant Rijhwani",
    author_email="dushyant@dush.me"
)
