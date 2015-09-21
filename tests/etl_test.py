# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 09:00:21 2015

@author: Robert Smith
"""

#import os
import yaml
import unittest
#import pyetl.api as api
from pyetl.api import convert_to_bool, LocalTable, TransformPipeline, ForeignTable, ETL


class Integration_Test(unittest.TestCase):

    def setUp(self):
        self.eng_url = 'postgresql+psycopg2://rws:rws@localhost:5432/central'
        self.sql_file = "c:/users/a421356/python/scripts/sql/edw/calendar.sql"
        self.foreign_path = "c:/users/a421356/python/scripts/config/edw/external_connect.yml"
        self.local_path = "c:/users/a421356/python/scripts/config/edw/local_engine.yml"
        self.transform_dict = {
            'cal_last_day_mo_ind': convert_to_bool('Y'),
            'cal_last_day_yr_ind': convert_to_bool('Y'),
            'sal_ind': convert_to_bool('Y'),
            'pal_ind': convert_to_bool('Y'),
            }

        with open(self.foreign_path) as fl:
            self.fdb = yaml.load(fl.read())
        with open(self.local_path) as fl:
            self.le = yaml.load(fl.read())
        with open(self.sql_file) as fl:
            self.sql = fl.read()

    def tearDown(self):
        self.eng_url = None
        self.sql_file = None
        self.foreign_path = None
        self.local_path = None


    def test_main(self):
        tfms = TransformPipeline(**self.transform_dict)
        lt = LocalTable(self.le, 'edw', 'calendar', tfms)
        ft = ForeignTable(self.fdb, self.sql)
        e = ETL(foreign_table=ft, local_table=lt)
        e.run()
        self.assertTrue(True)




if __name__ == "__main__":
    unittest.main()
