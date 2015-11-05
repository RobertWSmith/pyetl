# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 09:08:18 2015

@author: Robert Smith
"""

import yaml
import pyodbc
import os

from .generators import DateGenerator
from .transformations import named_tuple_factory



class ForeignDB(yaml.YAMLObject):
    """\
    Stores connection instructions for foreign database in `pickleable` format.
    Intended to be used in cases requiring `multiprocessing`.
    """

    yaml_tag = "!ForeignDB"

    def __init__(self, **kwargs):
        """\
        :param **kwargs: Any keyword arguments which can be passed to `pyodbc.connect`
        """
        self.kwargs = kwargs

    def connect(self):
        """\
        Returns new `pyodbc.Connection` object each time called.
        """
        return pyodbc.connect(**self.kwargs)



class ForeignTable(yaml.YAMLObject):
    """\
    Represents foreign data table to import into local database table.
    'Extract' portion of ETL process.
    """

    yaml_tag = "!ForeignTable"
    _sql = None
    sql_file = None
    sqlarg_generator = None
    output_dir = None
    _default_chunksize = 100000

    def __init__(self, foreign_db, sql_file, chunksize=None, sqlarg_generator=None):
        self.foreign_db = foreign_db
        self.sql_file = sql_file
        if chunksize:
            self.chunksize = chunksize
        else:
            self.chunksize = self._default_chunksize
        self.sqlarg_generator = sqlarg_generator

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(fields['foreign_db'], fields['sql_file'],
                   fields.get('chunksize', ForeignTable._default_chunksize), fields.get('sqlarg_generator'))

    def output_filepath(self, val=''):
        return os.path.join(self.output_dir, 'table_' + val + '.' + self.output_ext)

    @property
    def sql(self):
        if self._sql is None:
            if not os.path.isfile(self.sql_file):
                self.sql_file = os.path.abspath(self.sql_file)
            with open(self.sql_file, mode='r') as fl:
                self._sql = fl.read()
        return self._sql

    def __call__(self):
        return self.extract()

    def extract(self):
        conn = self.foreign_db.connect()
        if isinstance(self.sqlarg_generator, DateGenerator):
            for val in self.sqlarg_generator:
                qry = conn.execute(self.sql, val)
                hdr = tuple((str(v[0]).lower() for v in qry.description))
                nt = named_tuple_factory('table_row', field_names=hdr)
                res = qry.fetchmany(self.chunksize)
                while res:
                    yield list((nt(*val) for val in res))
                    res = qry.fetchmany(self.chunksize)
        else:
            qry = conn.execute(self.sql)
            hdr = tuple((str(v[0]).lower() for v in qry.description))
            nt = named_tuple_factory('table_row', field_names=hdr)
            res = qry.fetchmany(self.chunksize)
            while res:
                yield list((nt(*val) for val in res))
                res = qry.fetchmany(self.chunksize)
        conn.close()






