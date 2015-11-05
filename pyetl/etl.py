# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 14:12:25 2015

@author: Robert Smith
"""

import yaml

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
            where = self.local_table.table_alias.columns.snapshot_dt.between(self.foreign_table.sqlarg_generator.start_date, self.foreign_table.sqlarg_generator.end_date)
            print(where)
            self.local_table.delete(whereclause=where)
        else:
            self.local_table.delete()
        self.local_table.vacuum()
        self.local_table.bulk_insert(self.foreign_table.extract())
        self.local_table.reindex()
        self.local_table.vacuum(analyze=True)


