# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 18:49:31 2015

@author: Robert Smith
"""

from setuptools import setup, find_packages

setup(
    name="pyetl",
    version = "0.0.2",
    packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests",
                                      "*.egg-info"]),
    author = "Robert Smith",
    author_email = "rob_smith@goodyear.com",
    description = "utilities for extract, transform & load",
    install_requires = ['pyodbc >= 3.0', 'psycopg2 >= 2.6', 'python-dateutil >= 2.4',
                        'pyyaml >= 3.11'],
    test_suite = 'tests'
)
