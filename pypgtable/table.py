"""Application layer wrapper for raw_table."""


from copy import deepcopy
from json import load
from logging import NullHandler, getLogger
from os.path import join
from collections import namedtuple

from psycopg2.errors import ProgrammingError

from .raw_table import raw_table
from .utils.text_token import text_token

_logger = getLogger(__name__)
_logger.addHandler(NullHandler())


def _dynamic_decode(columns, _table, code):
    conversions = []
    for i, column in enumerate(columns):
        comment = f'# {column}'
        value = f'v[{i}],' if _table._conversions[column][code] is None else f'self.conversions[{i}](v[{i}]),'
        conversions.append(f'{value:<30} {comment}')
    return conversions


class tuple_iter():
    """Iterator returning a tuple decoded values from values.

    The order of the tuples returned is the same as the rows of values in values.
    Each value is decoded by the registered conversion function (see register_conversion()) or
    unchanged if no conversion has been registered.
    """

    def __init__(self, columns, values, _table, code='decode'):
        """Initialise.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (row_iter): Iterator over rows (tuples) with values in the order as columns.
        """
        self.values = values
        self.conversions = [_table._conversions[column][code] for column in columns]
        mapping = '\n'.join([f'\t\t{v}' for v in _dynamic_decode(columns, _table, code)])
        exec_str = f'def _next(self):\n\tv = next(self.values)\n\treturn (\n{mapping}\n\t)'
        _logger.debug(f'tuple_iter function:\n{exec_str}')
        scope={}
        exec(exec_str, locals(), scope)
        self._next = scope['_next']


    def __iter__(self):
        """Self iteration."""
        return self


    def __next__(self):
        """Replace by a dynamic function generated in __init__()."""
        return self._next(self)


class namedtuple_iter():
    """Iterator returning a namedtuple with columns keys and decoded values from values.

    The order of the tuples returned is the same as the rows of values in values.
    Each value is decoded by the registered conversion function (see register_conversion()) or
    unchanged if no conversion has been registered.
    """

    def __init__(self, columns, values, _table, code='decode'):
        """Initialise.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (row_iter): Iterator over rows (tuples) with values in the order as columns.
        """
        _next = None
        self.values = values
        self.conversions = [_table._conversions[column][code] for column in columns]
        self.namedtuple = namedtuple('row', columns)
        mapping = '\n'.join((f'\t\t{v}' for v in _dynamic_decode(columns, _table, code)))
        exec_str = f'def _next(self):\n\tv = next(self.values)\n\treturn self.namedtuple(\n{mapping}\n\t)'
        _logger.debug(f'namedtuple_iter function:\n{exec_str}')
        scope={}
        exec(exec_str, locals(), scope)
        self._next = scope['_next']


    def __iter__(self):
        """Self iteration."""
        return self


    def __next__(self):
        """Replace by a dynamic function generated in __init__()."""
        return self._next(self)


class dict_iter():
    """Iterator returning a dict with columns keys and decoded values from values.

    The order of the dicts returned is the same as the rows of values in values.
    Each value is decoded by the registered conversion function (see register_conversion()) or
    unchanged if no conversion has been registered.
    """

    def __init__(self, columns, values, _table, code='decode'):
        """Initialise.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (row_iter): Iterator over rows (tuples) with values in the order as columns.
        """
        _next = None
        self.values = values
        self.conversions = [_table._conversions[column][code] for column in columns]
        c_str = (f"'{c}':" for c in columns)
        mapping = '\n'.join((f"\t\t{k:<20}{v}" for k, v in zip(c_str, _dynamic_decode(columns, _table, code))))
        exec_str = f'def _next(self):\n\tv = next(self.values)\n\treturn {{\n{mapping}\n\t}}'
        _logger.debug(f'dict_iter function:\n{exec_str}')
        scope={}
        exec(exec_str, locals(), scope)
        self._next = scope['_next']


    def __iter__(self):
        """Self iteration."""
        return self


    def __next__(self):
        """Replace by a dynamic function generated in __init__()."""
        return self._next(self)


class table():
    """Wrap raw_table providing convinience functions for accessing & modifying a postgresql table."""

    def __init__(self, config):
        """Create a table object."""
        self.raw = raw_table(config, populate=False)
        self._entry_validator = None
        self._conversions = {column: {'encode': None, 'decode': None} for column in self.raw.config['schema']}
        self._conversions.update({c: {'encode': e, 'decode': d} for c, e, d in self.raw.config['conversions']})
        self._populate_table()

    def __getitem__(self, pk_value):
        """Query the table for the row with primary key value pk_value.

        Args
        ----
        pk_value (obj): A primary key value.

        Returns
        -------
        (dict) with the row values or an empty dict if the primary key does not exist.
        """
        if self.raw._primary_key is None:
            raise ValueError("SELECT row on primary key but no primary key defined!")
        encoded_pk_value = self.encode_value(self.raw._primary_key, pk_value)
        try:
            return next(self.select('WHERE {' + self.raw._primary_key + '} = {_pk_value}', {'_pk_value': encoded_pk_value}))
        except StopIteration:
            raise KeyError


    def __setitem__(self, pk_value, values):
        """Upsert the row with primary key pk_value using values.

        If values contains a different primary key value to pk_value, pk_value will
        override it.

        Args
        ----
        pk_value (obj): A primary key value.
        values (obj): A dict of column:value
        """
        new_values = deepcopy(values)
        new_values[self.raw._primary_key] = pk_value
        self.upsert((new_values,))

    def __len__(self):
        """Count the number of rows in the table."""
        return len(self.raw)

    def _populate_table(self):
        """Add data to table after creation.

        The JSON file must be a list of dicts.
        The dicts (rows) will be inserted into the table using batches of contiguous rows
        with the exactly the same fields defined i.e. this allows you to leave fields in some
        rows undefined and use the database table default. However, it breaks the inserts into
        smaller batches than may be otherwise possible reducing the overall performance. If order
        does not matter arrange your JSON file into the minimum number of contiguous matching rows.
        The intention of this process is to maintain the order of rows in the from the JSON file.

        Dict keys that are not table columns will be ignored.
        Only executed if this instance of raw_table() created it.
        See self._create_table().
        """
        if self.raw.creator and self.raw.config['data_files']:
            for data_file in self.raw.config['data_files']:
                abspath = join(self.raw.config['data_file_folder'], data_file)
                _logger.info(text_token({'I05004': {'table': self.raw.config['table'], 'file': abspath}}))
                with open(abspath, "r") as file_ptr:
                    self.insert(load(file_ptr))

    def columns(self):
        """Return a tuple of all column names."""
        return self.raw._columns

    def _return_container(self, columns, values, container='dict'):
        if columns == '*':
            columns = self.raw._columns
        if container == 'tuple':
            return tuple_iter(columns, values, self)
        if container == 'namedtuple':
            return namedtuple_iter(columns, values, self)
        return dict_iter(columns, values, self)

    def register_conversion(self, column, encode_func, decode_func):
        """Define functions to encode column into the table and decode it out.

        Typically decode_func(encode_func(x)) == x and encode_func(decode_func(y)) == y
        however this is not a requirement (Good luck).

        Args
        ----
        column (str): A column in the table.
        encode_func (f()): Takes a single object x and returns a single encoded object. The returned
            object must have the same type as the table column. e.g. lambda x: compress(x)
        decode_func (f()): Takes a single object y and returns a single decoded object. y is the raw value
            returned from the table column. e.g. lambda y: decompress(y)
        """
        self._conversions[column]['encode'] = encode_func
        self._conversions[column]['decode'] = decode_func

    def encode_value(self, column, value):
        """Encode value using the registered conversion function for column.

        If no conversion function is registered value is returned unchanged.
        This function is provided to create encoded literals in query functions.

        Args
        ----
        column (str): Column name for value.
        value  (obj): Value to encode.

        Returns
        -------
        (obj): Encoded value
        """
        conversion = self._conversions[column]['encode']
        return conversion(value) if conversion is not None else value


    def select(self, query_str='', literals={}, columns='*', container='dict'):
        """Select columns to return for rows matching query_str.

        Args
        ----
        query_str (str): Query SQL: SQL starting 'WHERE ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {one} ORDER BY {column1} ASC' where 'column1' is a column name and 'one' is a key
            in literals. If literals = {'one': 1}, columns = ('column1', 'column3') and the table name is
            'test_table' the example query_str would result in the following SQL:
                SELECT "column1", "column3" FROM "test_table" WHERE "column1" = 1 ORDER BY "column1" ASC
        literals (dict): Keys are labels used in query_str. Values are literals to replace the labels.
            NOTE: Literal values for encoded columns must be encoded. See encode_value().
        columns (iter): The columns to be returned on update. If '*' defined all columns are returned
            in the order of self.columns()
        container (str): Defines the type of container in the returned list. Set as either
            'tuple': Returns an iterator that returns tuples where values in the tuples are in the order of columns.
            'namedtuple': Returns an iterator that returns namedtuples where values in the namedtuples are in the
                order of columns & have column names.
            Any other value: Returns an iterator that returns dicts where the keys are column names.

        Returns
        -------
        (iterator('container')): An iterator of the values specified by columns for the specified query_str.
        """
        return self._return_container(columns, self.raw.select(query_str, literals, columns), container)

    def recursive_select(self, query_str='', literals={}, columns='*', container='dict'):
        """Recursive select of columns to return for rows matching query_str.

        Recursion is defined by the ptr_map (pointer map) in the table config.
        If the rows in the table define nodes in a graph then the pointer map defines
        the edges between nodes.

        self.config['ptr_map'] is of the form {
            "column X": "column Y",
            ...
        }
        where column X contains a reference to a node identified by column Y.

        Recursive select will return all the rows defined by the query_str plus the union of any rows
        they point to and the rows those rows point to...recursively until no references are left (or
        are not in the table).

        Args
        ----
        query_str (str): Query SQL: See select() for details.
        literals (dict): Keys are labels used in query_str. Values are literals to replace the labels.
            NOTE: Literal values for encoded columns must be encoded. See encode_value().
        columns (iter): The columns to be returned on update. If '*' defined all columns are returned.
        container (str): Defines the type of container in the returned list. Set as either
            'tuple': Returns an iterator that returns tuples where values in the tuples are in the order of columns.
            'namedtuple': Returns an iterator that returns namedtuples where values in the namedtuples are in the
                order of columns & have column names.
            Any other value: Returns an iterator that returns dicts where the keys are column names.

        Returns
        -------
        (iterator('container')): An iterator of the values specified by columns for the specified recursive query_str
            and pointer map.
        """
        return self._return_container(columns, self.raw.recursive_select(query_str, literals, columns), container)

    def upsert(self, values_dict, update_str=None, literals={}, returning=tuple(), container='dict', exclude=tuple()):
        """Upsert values.

        If update_str is None each entry will be inserted or replace the existing entry on conflict.
        In this case literals is not used.

        Args
        ----
        values_dict (iter(dict)): Keys are column names. Values will be encoded by the registered conversion
            function (if any).
        update_str (str): Update SQL: SQL after 'UPDATE SET ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {EXCLUDED.column1} + {one}' where 'column1' is a column name and 'one' is a key
            in literals. Prepend 'EXCLUDED.' to read the existing value. If values_dict=({'column1': 10},),
            literals = {'one': 1} and the table name is 'test_table' the example update_str
            would result in the following SQL:
                INSERT INTO "test_table" "column1" VALUES(10) ON CONFLICT DO
                    UPDATE SET "column1" = EXCLUDED."column1" + 1
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.
            NOTE: Literal values for encoded columns must be encoded. See encode_value().
        returning (iter): The columns to be returned on update. If None or empty no columns will be returned.
        container (str): Defines the type of container in the returned list. Set as either
            'tuple': Returns a list(tuple) where tuples are in the order of columns.
            'list': Returns a list(list) where sub-lists are in the order of columns.
            'pkdict': Returns a list(dict(dict)) where the first dict is is a dict of primary keys
                and the second a dict of columns.
            any other value returns list(dicts) with the specified columns.
        exclude (iter(str)): Iterable of columns to exclude from insert.

        Returns
        -------
        (iterator('container')): An iterator of the values specified by returning for each updated row.
        """
        retval = []
        for columns, values in self.raw.batch_dict_data(values_dict, exclude):
            results = self.raw.upsert(columns, tuple_iter(columns, iter(values), self, 'encode'), update_str, literals, returning)
            if returning:
                retval.extend(results)
        return self._return_container(returning, iter(retval), container)

    def insert(self, values_dict, exclude=tuple()):
        """Insert values.

        Args
        ----
        values_dict (iter(dict)): Keys are column names. Values will be encoded by the registered conversion
            function (if any).
        exclude (iter(str)): Iterable of columns to exclude.
        """
        for columns, values in self.raw.batch_dict_data(values_dict, exclude):
            self.raw.insert(columns, tuple_iter(columns, iter(values), self, 'encode'))

    def update(self, update_str, query_str, literals={}, returning=tuple(), container='dict'):
        """Update rows.

        Each row matching the query_str will be updated by the update_str.

        Args
        ----
        update_str (str): Update SQL: SQL after 'SET ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {column1} + {one}' where 'column1' is a column name and 'one' is a key
            in literals. The table identifier will be appended to any column names. If literals =
            {'one': 1, 'nine': 9}, query_str = 'WHERE {column2} = {nine}' and the table name is 'test_table' the
            example update_str would result in the following SQL:
                UPDATE "test_table" SET "column1" = "column1" + 1 WHERE "column2" = 9
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.
            NOTE: Literal values for encoded columns must be encoded. See encode_value().
        returning (iter): An iterable of column names to return for each updated row.
        container (str): Defines the type of container in the returned list. Set as either
            'tuple': Returns a list(tuple) where tuples are in the order of columns.
            'list': Returns a list(list) where sub-lists are in the order of columns.
            'pkdict': Returns a list(dict(dict)) where the first dict is is a dict of primary keys
                and the second a dict of columns.
            any other value returns list(dicts) with the specified columns.

        Returns
        -------
        (iterator('container')): An iterator of the values specified by returning for each updated row or [] if returning is
            an empty iterable or None.
        """
        return self._return_container(returning, self.raw.update(update_str, query_str, literals, returning), container)

    def delete(self, query_str, literals={}, returning=tuple(), container='dict'):
        """Delete rows from the table.

        If query_str is not specified all rows in the table are deleted.

        Args
        ----
        query_str (str): Query SQL: SQL after 'DELETE FROM table WHERE ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {value}' where 'column1' is a column name, literals = {'value': 72}, ret=False and the table name
            is 'test_table' the example query_str would result in the following SQL:
                DELETE FROM "test_table" WHERE "column1" = 72
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.
            NOTE: Literal values for encoded columns must be encoded. See encode_value().
        returning (iter): An iterable of column names to return for each deleted row.
        container (str): Defines the type of container in the returned list. Set as either
            'tuple': Returns a list(tuple) where tuples are in the order of columns.
            'list': Returns a list(list) where sub-lists are in the order of columns.
            'pkdict': Returns a list(dict(dict)) where the first dict is is a dict of primary keys
                and the second a dict of columns.
            any other value returns list(dicts) with the specified columns.

        Returns
        -------
        (iterator('container')): An iterator of the values specified by returning for each updated row or [] if returning is
            an empty iterable or None.
        """
        return self._return_container(returning, self.raw.delete(query_str, literals, returning), container)
