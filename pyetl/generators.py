# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 08:31:22 2015

@author: Robert Smith
"""

import datetime
import yaml
from dateutil.relativedelta import relativedelta


class DateGenerator(yaml.YAMLObject):

    yaml_tag = "!DateGenerator"
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    first_of_month = datetime.date(yesterday.year, yesterday.month, 1)
    base_interval = datetime.timedelta(weeks=2)
    start_date = None
    end_date = None
    iter_interval = None

    def __init__(self, start_date,
                 end_date=datetime.date.today() - datetime.timedelta(days=1),
                 iter_interval=datetime.timedelta(weeks=2)):
        self.start_date = start_date
        self.end_date = end_date
        self.iter_interval = iter_interval

    def __iter__(self):
        self._iter_start = self.start_date
        return self

    def __next__(self):
        if self._iter_start > self.end_date:
            del self._iter_start
            raise StopIteration
        local_start = self._iter_start
        local_end = self._iter_start + self.iter_interval
        if local_end > self.end_date:
            local_end = self.end_date
        self._iter_start = local_end + datetime.timedelta(days=1)
        return (local_start, local_end)

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        start_date = fields['start_date']
        if isinstance(start_date, str) and start_date == 'first of month':
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            first_of_month = datetime.date(yesterday.year, yesterday.month, 1)
            if yesterday.day < 7:
                first_of_month = first_of_month - relativedelta(months=1)
            start_date = first_of_month
        end_date = fields.get('end_date', DateGenerator.yesterday)
        iter_interval = fields.get('iter_interval', DateGenerator.base_interval)
        if iter_interval == 'month':
            iter_interval = relativedelta(months=1) - relativedelta(days=1)
        _class = cls(start_date, end_date, iter_interval)
        return _class

