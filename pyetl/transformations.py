# -*- coding: utf-8 -*-
"""
Created on Thu Sep 10 08:17:25 2015

@author: Robert Smith
"""


from decimal import Decimal
import datetime
import yaml
from collections import namedtuple


def named_tuple_factory(typename, field_names, verbose=False, rename=False):

    class NamedTuple(namedtuple(typename, field_names, verbose, rename)):

        def keys(self):
            return self._fields

        def __getitem__(self, key):
            if isinstance(key, str):
                return getattr(self, key)
            else:
                return super().__getitem__(key)

        def __setitem__(self, key, value):
            return self._replace(**{str(key): value})

        def __contains__(self, key):
            return str(key) in self._fields

    return NamedTuple


class TransformPipeline(yaml.YAMLObject):

    yaml_tag = "!TransformPipeline"
    transforms = {}

    def __init__(self, **transforms):
        if transforms is not None and len(dict(transforms)) > 0:
            self.transforms = dict(transforms)

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(**fields)

    def __call__(self, row):
        return self.convert(row)

    def convert(self, row):
        for key, value in self.transforms.items():
            if key in row:
                row[key] = self.transforms[key](row[key])
        return row


class ConvertTo(yaml.YAMLObject):
    """\
    Base class for required string conversions.
    """
    yaml_tab = '!ConvertTo'

    def convert(self, value):
        raise NotImplementedError

    def __call__(self, value):
        return self.convert(value)


class convert_to_bool(ConvertTo):

    yaml_tag = "!convert_to_bool"

    def __init__(self, true_value):
        """\
        Converts value to boolean
        """
        self.true_value = true_value

    def convert(self, value):
        if value is None or value == '':
            return None
        else:
            return self.true_value == value


class convert_to_decimal(ConvertTo):

    yaml_tag = "!convert_to_decimal"
    _precision = None

    def __init__(self, precision=3):
        self.precision = precision

    @classmethod
    def from_yaml(cls, loader, node):
        fields = loader.construct_mapping(node, deep=True)
        return cls(fields['precision'])

    @property
    def precision(self):
        return self._precision

    @precision.setter
    def precision(self, value):
        self._precision = Decimal(value)

    @property
    def quantize_scale(self):
        return Decimal(10) ** (-1 * self.precision)

    def convert(self, value):
        return Decimal(value).quantize(self.quantize_scale)


class convert_to_int(ConvertTo):

    yaml_tag = '!convert_to_int'

    def convert(self, value):
        return int(value)


class convert_to_date(ConvertTo):

    yaml_tag = "!convert_to_date"

    def __init__(self, fmt_str = "%Y-%m-%d"):
        self.fmt_str = fmt_str

    def convert(self, value):
        if value is None or value == '':
            return None
        else:
            return datetime.datetime.strptime(value, self.fmt_str).date()


class convert_to_time(ConvertTo):

    yaml_tag = "!convert_to_time"

    def __init__(self, fmt_str = "%H:%M:%S"):
        self.fmt_str = fmt_str

    def convert(self, value):
        if value is None or value == '':
            return None
        else:
            return datetime.datetime.strptime(value, self.fmt_str).time()


class convert_to_datetime(ConvertTo):

    yaml_tag = "!convert_to_datetime"

    def __init__(self, fmt_str = "%Y-%m-%d %H:%M:%S"):
        self.fmt_str = fmt_str

    def convert(self, value):
        if value is None or value == '':
            return None
        else:
            return datetime.datetime.strptime(value, self.fmt_str)


if __name__ == '__main__':
    yaml_str = """\
calendar: !Transformations
    transforms:
        cal_last_day_mo_ind: !string_to_bool {true_value: Y}
        cal_last_day_yr_ind: !string_to_bool {true_value: Y}
material: !Transformations
    transforms:
        sal_ind: !string_to_bool {true_value: Y}
        pal_ind: !string_to_bool {true_value: Y}

"""
    ld = yaml.load(yaml_str)
    print(repr(ld))


