"""Application layer wrapper for raw_table."""


from copy import deepcopy
from os.path import join
from json import load
from .raw_table import raw_table
from cerberus import Validator


_IDENTITY_FUNC = lambda x: x


class table():
    """A wrapper for raw_table providing convinience functions for accessing & modifying a postgresql table."""


    def __init__(self, config):
        """Create a table object."""
        self.raw = raw_table(config)
        self._entry_validator = None
        self._conversions = {column: {'encode': _IDENTITY_FUNC, 'decode': _IDENTITY_FUNC} for column in config['columns']} 


    def __getitem__(self, pk_value):
        """Query the table for the row with primary key value pk_value.
        
        Args
        ----
        pk_value (obj): A primary key value.

        Returns
        -------
        (dict) with the row values or an empty dict if the primary key does not exist.
        """
        if self.raw._primary_key is None: raise ValueError('SELECT row on primary key but no primary key defined!')
        pk_value = self.encode_value(self.raw._primary_key, pk_value)
        return self.select(['{' + self._primary_key + '} = {_pk_value}', {'_pk_value': pk_value}]).fetchone()[0]


    def __setitem__(self, pk_value, values):
        """Upsert the row with primary key pk_value using values.

        If values contains a different primary key value to pk_value, pk_value will
        override it.
        
        Args
        ----
        pk_value (obj): A primary key value.
        values (obj): A dict of column:value
        """
        if self.raw._primary_key is None: raise ValueError('SELECT row on primary key but no primary key defined!')
        new_values = deepcopy(values)
        new_values[self.raw._primary_key] = pk_value
        self.upsert(new_values)


    def _return_container(self, columns, values, container='dict'):
        if container == 'tuple': return self.decode_values_to_tuple(columns, values)
        if container == 'list': return self.decode_values_to_list(columns, values)
        return self.decode_values_to_dict(columns, values)


    def register_converstion(self, column, encode_func, decode_func):
        self._conversions[column]['encode'] = encode_func
        self._conversions[column]['decode'] = decode_func


    def decode_values_to_dict(self, columns, values):
        return [{column: self._conversions[column]['decode'](value) for column, value in zip(columns, row)} for row in values]


    def decode_values_to_list(self, columns, values):
        return [[self._conversions[column]['decode'](value) for column, value in zip(columns, row)] for row in values]


    def decode_values_to_tuple(self, columns, values):
        return [tuple((self._conversions[column]['decode'](value) for column, value in zip(columns, row))) for row in values]


    def encode_values_to_tuple(self, columns, values):
        return [tuple((self._conversions[column]['encode'](value) for column, value in zip(columns, row))) for row in values]


    def encode_value(self, column, value):
        return self._conversions[column]['encode'](value)


    def select(self, query_str='', literals={}, columns=None, repeatable=False, container='dict'):
        values = self.raw.select(query_str, literals, columns, repeatable)
        return self._return_container(columns, values, container)


    def select_recursive(self, query_str='', literals={}, columns=None, repeatable=False, container='dict'):
        values = self.raw.recursive_select(query_str, literals, columns, repeatable)
        return self._return_container(columns, values, container)


    def upsert(self, values_dict, update_str=None, literals={}, returning=tuple(), container='dict'):
        retval = []
        for columns, values in self.raw.batch_dict_data(values_dict):
            encoded_values = self.encode_values_to_tuple(columns, values)
            retval.extend(self.raw.upsert(columns, encoded_values, update_str, literals, returning))
        return self._return_container(returning, retval, container)


    def insert(self, values_dict):
        for columns, values in self.raw.batch_dict_data(values_dict):
            encoded_values = self.encode_values_to_tuple(columns, values)
            self.raw.insert(columns, encoded_values)


    def update(self, update_str, query_str, literals={}, returning=tuple(), container='dict'):
        retval = self.raw.update(update_str, query_str, literals, returning)
        return self._return_container(returning, retval, container)


    def delete(self, query_str, literals={}, returning=tuple(), container='dict'):
        retval = self.raw.delete(query_str, literals, returning)
        return self._return_container(returning, retval, container)


    def validate(self, values_dict):
        """Validate entries.

        Entries are blindly validated (return True for all) if the table configuration
        does not have a format_file defined.

        Args
        ----
        entries (list(dict)): List of entries to validate.

        Returns
        -------
        (tuple(bool)): Tuple with the same length as entries with the validity of each entry.
        """
        if 'format_file' in self.config:
            if self._entry_validator is None:
                schema_path = join(self.config['format_file_folder'], self.config['format_file'])
                with open(schema_path, "r") as schema_file:
                    self._entry_validator = Validator(load(schema_file))
            return tuple((self._entry_validator(value) for value in values_dict))
        return (True,) * len(values_dict)
