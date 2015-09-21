# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 09:08:18 2015

@author: Robert Smith
"""

#import itertools
import yaml
import pyodbc
#import logging
#import itertools

import os
import glob


from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import ProgrammingError

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



class LocalEngine(yaml.YAMLObject):
    """\
    Stores engine string for `sqlalchemy.create_engine`. Allows for string with
    {} formatting syntax with named parameters.
    """

    yaml_tag = "!LocalEngine"

    def __init__(self, engine_str, **fmt_kwargs):
        """\
        :param engine_str: either fully formed engine url, or formatting string with named keyword argyments
        :param **fmt_kwargs: keyword arguments to be unpacked for engine_str, if not passed engine_str assumed to be useable as-is.
        :type engine_str: `str`
        :type **fmt_kwargs: `dict` of `str` keys and values, optional
        """
        self.engine_str = engine_str
        self.fmt_kwargs = fmt_kwargs

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(fields['engine_str'], **fields['fmt_kwargs'])

    @property
    def engine_url(self):
        """\
        Returns `engine_str` if `fmt_kwargs` are not provided, else formats
        `engine_str` to one engine URL.
        """
        if len(self.fmt_kwargs) > 0:
            return self.engine_str.format(**self.fmt_kwargs)
        else:
            return self.engine_str

    def create_engine(self):
        """\
        Returns new `sqlalchemy.Engine` each time called.
        """
        return create_engine(self.engine_url)



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

    def __init__(self, foreign_db, sql_file, output_dir, output_ext,
                 output_dialect, sqlarg_generator=None):
        self.foreign_db = foreign_db
        self.sql_file = sql_file
        self.output_dir = output_dir
        self.output_ext = output_ext
        self.output_dialect = output_dialect
        self.sqlarg_generator = sqlarg_generator

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(fields['foreign_db'], fields['sql_file'], fields['output_dir'],
                   fields['output_ext'], fields['output_dialect'], fields.get('sqlarg_generator'))

    @property
    def sql(self):
        if self._sql is None:
            with open(self.sql_file, mode='r') as fl:
                self._sql = fl.read()
        return self._sql

    def __call__(self):
        return self.extract()

    def extract(self):
        if os.path.isdir(self.output_dir):
            for filename in glob.glob(os.path.join(self.output_dir, '*.txt')):
                os.remove(filename)
        else:
            os.mkdir(self.output_dir)

        conn = self.foreign_db.connect()
        if isinstance(self.sqlarg_generator, DateGenerator):
            for val in self.sqlarg_generator:
                qry = conn.execute(self.sql, val)
                hdr = tuple((str(v[0]).lower() for v in qry.description))
                nt = named_tuple_factory('table_row', field_names=hdr)
                res = qry.fetchmany(100000)
                while res:
                    yield list((nt(*val) for val in res))
                    res = qry.fetchmany(100000)
        else:
            qry = conn.execute(self.sql)
            hdr = tuple((str(v[0]).lower() for v in qry.description))
            nt = named_tuple_factory('table_row', field_names=hdr)
            res = qry.fetchmany(100000)
            while res:
                yield list((nt(*val) for val in res))
                res = qry.fetchmany(100000)
        conn.close()



class LocalTable(yaml.YAMLObject):
    """\
    Local table details for pre-bulk insertion maintenance, bulk insertion of
    records and post-bulk insertion cleanup.
    """

    yaml_tag = "!LocalTable"
    _metadata = MetaData()
    _engine = None
    transformer = None

    def __init__(self, local_engine, schema, tablename, input_dialect, transformer=None):
        self.transformer = transformer
        self.local_engine = local_engine
        self.schema = schema
        self.tablename = tablename
        self.input_dialect = input_dialect

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(fields['local_engine'], fields['schema'], fields['tablename'],
                   fields['input_dialect'], fields.get('transformer'))

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self.local_engine.create_engine()
        return self._engine

    @property
    def metadata(self):
        self._metadata.reflect(bind=self.engine, schema=self.schema)
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value
        self._metadata.reflect(bind=self.engine, schema=self.schema)

    @property
    def table(self):
        return Table(self.tablename, self.metadata, autoload=True, autoload_with=self.engine)

    @property
    def columns(self):
        return self.table.columns

#    def upsert(self, *rows):
#        """\
#        Tries to delete record based on table's primary key fields, then inserts.
#        """
#        eng = self.local_engine.create_engine()
#        conn = eng.connect()
#        trans = conn.begin()
#        key_col_name = []
#        for val in self.table.primary_key:
#            key_col_name.append(val.name)
#        for row in rows:
#            try:
#                conn.execute(self.table.delete(), **{key: row[key] for key in key_col_name})
#            except ProgrammingError:
#                pass
#            finally:
#                conn.execute(self.table.insert(), **row)
#        trans.commit()
#        conn.close()

    def transform(self, row):
        """\
        Attempts to call `transformer` member to convert row as-needed. If
        `transformer` is not initialized, does not modify row.
        """
        if self.transformer is not None:
            return self.transformer(row)
        else:
            return dict(row)

    def bulk_insert(self, outputs):
        """\
        Inserts all records in iterable sequence passed as argument.

        Args:
            *rows iterable, with each value being `dict` with keys corresponding to the field names of the table
        """
        for chunk in outputs:
            conn = self.engine.connect()
            print(len(chunk))
            trans = conn.begin()
            conn.execute(self.table.insert(), list([self.transform(row) for row in chunk]))
            trans.commit()
            conn.close()

    def delete(self, whereclause=None, **kwargs):
        """\
        Mostly for bulk deletes (whole table, etc.)
        """
        conn = self.engine.connect()
        trans = conn.begin()
        conn.execute(self.table.delete(whereclause, **kwargs))
        trans.commit()
        conn.close()

    def drop_indexes(self):
        eng = self.local_engine.create_engine()
        for ix in self.table.indexes:
            try:
                ix.drop(eng)
            except ProgrammingError:
                pass

    def reindex(self):
        eng = self.local_engine.create_engine()
        for ix in self.table.indexes:
            try:
                ix.create(eng)
            except ProgrammingError:
                pass

    def vacuum(self, analyze=False):
        eng = self.local_engine.create_engine()
        conn = eng.connect()
        orig_iso_lvl = conn.connection.isolation_level
        conn.connection.set_isolation_level(0)
        if analyze:
            sql_str = 'VACUUM ANALYZE {schema}.{tablename};'
        else:
            sql_str = 'VACUUM {schema}.{tablename};'
        conn.execute(sql_str.format(schema=self.schema, tablename=self.tablename))
        conn.connection.set_isolation_level(orig_iso_lvl)
        conn.close()



class ETL(yaml.YAMLObject):

    yaml_tag = '!ETL'

    def __init__(self, foreign_table, local_table):
        self.foreign_table = foreign_table
        self.local_table = local_table

    def __call__(self):
        return self.run()

    def run(self):
        self.local_table.drop_indexes()
        if self.foreign_table.sqlarg_generator:
            print(self.foreign_table.sqlarg_generator.start_date)
            print(self.foreign_table.sqlarg_generator.end_date)
            where = self.local_table.columns.snapshot_dt.between(self.foreign_table.sqlarg_generator.start_date, self.foreign_table.sqlarg_generator.end_date)
            print(where)
            self.local_table.delete(whereclause=where)
        else:
            self.local_table.delete()
        self.local_table.vacuum()
        self.local_table.bulk_insert(self.foreign_table.extract())
        self.local_table.reindex()
        self.local_table.vacuum(analyze=True)


