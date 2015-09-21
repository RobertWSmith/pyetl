# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 08:31:22 2015

@author: Robert Smith
"""

import datetime
import yaml

#import copy
import dateutil
#import enum



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
        if isinstance(start_date, str):
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            first_of_month = datetime.date(yesterday.year, yesterday.month, 1)
            if start_date == 'first of month':
                if yesterday.day < 5:
                    first_of_month = dateutil.relativedelta.relativedelta(months=1)
                start_date = first_of_month
        end_date = fields.get('end_date', DateGenerator.yesterday)
        iter_interval = fields.get('iter_interval', DateGenerator.base_interval)
        _class = cls(start_date, end_date, iter_interval)
        return _class


#class MonthToDate(DateGenerator):
#
#    yaml_tag = '!MonthToDate'
#
#    def __init__(self):
#        begin_date = datetime.date(MonthToDate.yesterday.year, MonthToDate.yesterday.month, 1)
#        if MonthToDate.yesterday.day < 5:
#            begin_date = begin_date - dateutil.relativedelta.relativedelta(months=1)
#        super().__init__(begin_date, MonthToDate.yesterday, MonthToDate.base_interval)
#
#    @classmethod
#    def from_yaml(cls, loader, node):
#        _class = super().__new__(cls, DateGenerator.first_of_month, DateGenerator.yesterday, DateGenerator.base_interval)
#        return _class


#class EpochToDate(DateGenerator):
#
#    yaml_tag = '!EpochToDate'
#
#    def __init__(self, start_date, iter_interval=datetime.timedelta(weeks=2)):
#        super().__init__(start_date, EpochToDate.yesterday, iter_interval)
#
#    @classmethod
#    def from_yaml(cls, loader, node):
#        fields = loader.construct_mapping(node, deep=True)
#        _class = cls(fields['start_date'], fields.get('iter_interval', DateGenerator.base_interval))
#        _class.start_date = fields['start_date']
#        _class._iter_start = _class.start_date
#        return _class



