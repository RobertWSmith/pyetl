# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 18:49:31 2015

@author: Robert Smith
"""

from setuptools import setup, find_packages

setup(
    name="pyetl",
    version = "0.0.1",
    packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "*.egg-info"]),
    author = "Robert Smith",
    author_email = "rob_smith@goodyear.com",
    description = "utilities for extract, transform & load",
    install_requires = ['pyodbc', 'psycopg2'],
    test_suite = 'tests'
)
