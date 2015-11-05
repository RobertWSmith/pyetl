# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 14:10:34 2015

@author: Robert Smith
"""

import yaml
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import ProgrammingError, OperationalError


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


class LocalTable(yaml.YAMLObject):
    """\
    Local table details for pre-bulk insertion maintenance, bulk insertion of
    records and post-bulk insertion cleanup.
    """

    yaml_tag = "!LocalTable"
    copy_syntax = """COPY INTO {schema}.{tablename} FROM '{filepath}' USING DELIMITERS '\t','\n','"' NULL AS '' LOCKED;"""

    _metadata = MetaData()
    _engine = None
    transformer = None

    def __init__(self, local_engine, declarative_table, input_dialect, transformer=None):
        self.transformer = transformer
        self.local_engine = local_engine
        self.declarative_table = declarative_table
        self.input_dialect = input_dialect

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        _class = cls(fields['local_engine'], fields['declarative_table'], fields['input_dialect'], fields.get('transformer'))
        _class.copy_syntax = LocalTable.copy_syntax
        return _class
    @property
    def engine(self):
        if self._engine is None:
            self._engine = self.local_engine.create_engine()
        return self._engine

    @property
    def metadata(self):
        return self._metadata

    @property
    def schema(self):
        return self.table.schema

    @property
    def tablename(self):
        return self.table.name

    @property
    def table(self):
        return self.declarative_table.__table__

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
        if self.transformer:
            return self.transformer(row)
        else:
            return row

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
            conn.execute(self.table.insert(), [self.transform(row) for row in chunk])
            trans.commit()
            conn.close()

    @property
    def table_alias(self):
        return self.table.alias(name=self.tablename)

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
            except (ProgrammingError, OperationalError):
                pass

    def reindex(self):
        eng = self.local_engine.create_engine()
        for ix in self.table.indexes:
            ix.create(eng)

    def vacuum(self, analyze=False):
        eng = self.local_engine.create_engine()
        conn = eng.connect()
        if hasattr(conn.connection, 'isolation_level'):
            orig_iso_lvl = conn.connection.isolation_level
            conn.connection.set_isolation_level(0)
        if analyze:
            sql_str = 'VACUUM ANALYZE {schema}.{tablename};'
        else:
            sql_str = 'VACUUM {schema}.{tablename};'
            if eng.driver == "monetdb":
                sql_str = """CALL VACUUM('{schema}', '{tablename}');"""
        conn.execute(sql_str.format(schema=self.schema, tablename=self.tablename))
        if hasattr(conn.connection, 'isolation_level'):
            conn.connection.set_isolation_level(orig_iso_lvl)
        conn.close()

