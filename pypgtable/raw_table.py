"""Simplified database table access."""

from logging import getLogger, DEBUG
from copy import deepcopy
from os.path import join
from json import load
from time import sleep
from psycopg2 import sql, errors, ProgrammingError
from cerberus import Validator
from .database import db_transaction, db_connect, db_exists, db_create, db_delete
from .common import backoff_generator
from .validators import raw_table_config_validator, raw_table_column_config_validator as rtccv
from .utils.text_token import text_token, register_token_code


_logger = getLogger('pypgtable')
def _logit(): return _logger.getEffectiveLevel() == DEBUG


register_token_code("I05000", "SQL: {sql}")
register_token_code("I05001", "Table {table} cannot be created as it already exists in database {dbname}.")
register_token_code("I05002", "User {user} does not have privileges to create a table in database {dbname}.")
register_token_code("I05003", "Table {table} in database {dbname} does not yet exist. Waiting {backoff:.2}s to retry.")
register_token_code("I05004", "Adding data to table {table} from {file}.")
register_token_code("E05000", "Configuration error: {error}")
register_token_code("E05001", "{set} columns differ between DB {dbname} and table {table} configuration.")


_INITIAL_DELAY = 0.125
_BACKOFF_STEPS = 13
_BACKOFF_FUZZ = True
_TABLE_LEN_SQL = sql.SQL("SELECT COUNT(*) FROM {0}")
_TABLE_EXISTS_SQL = sql.SQL(
    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {0})")
_TABLE_DEFINITION_SQL = sql.SQL(
    "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = {0}")
_TABLE_GET_PRIMARY_KEY_SQL = sql.SQL("SELECT c.column_name, tc.constraint_type FROM information_schema.table_constraints tc "
    "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) "
    "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema "
    "AND tc.table_name = c.table_name AND ccu.column_name = c.column_name "
    "WHERE tc.table_name = {0}")
_TABLE_CREATE_SQL = sql.SQL("CREATE TABLE {0} ({1})")
_TABLE_INDEX_SQL = sql.SQL("CREATE INDEX {0} ON {1}")
_TABLE_INDEX_COLUMN_SQL = sql.SQL("({0})")
_TABLE_DELETE_TABLE_SQL = sql.SQL("DROP TABLE IF EXISTS {0} CASCADE")
_TABLE_RECURSIVE_SELECT = sql.SQL(
    "WITH RECURSIVE rq AS (SELECT {0} FROM {1} {2} UNION SELECT {3} FROM {1} t INNER JOIN rq r ON {4}) SELECT * FROM rq")
_TABLE_SELECT_SQL = sql.SQL("SELECT {0} FROM {1} {2}")
_TABLE_INSERT_SQL = sql.SQL("INSERT INTO {0} ({1}) VALUES {2} ON CONFLICT ")
_TABLE_INSERT_CONFLICT_STR = "DO NOTHING"
_TABLE_UPSERT_CONFLICT_STR = "{0} DO UPDATE SET "
_TABLE_UPDATE_SQL = sql.SQL("UPDATE {0} SET {1} WHERE {2}")
_TABLE_DELETE_SQL = sql.SQL("DELETE FROM {0} WHERE {1}")
_TABLE_RETURNING_SQL = sql.SQL(" RETURNING ")
_DEFAULT_UPDATE_STR = "{{{0}}}={{EXCLUDED.{0}}}"


class raw_table():
    """Connects to (or creates as needed) a postgres database & table.

    The intention of raw_table is to provide a simple interface to instanciate,
    append, update & query a persistant data store using directly mapped database types.

    Whilst database_table acts like it has complete control over the defined databases
    it does not assume that it does. Once tables are created raw_table users
    need only have SELECT, INSERT & UPDATE privileges.
    """

    def __init__(self, config):
        """Connect to or create all required objects.

        Args
        ----
        config (dict): The table configuration. See database/formats/raw_table_config_format.json.
        """
        self.config = deepcopy(config)
        self._validate_config()
        self._table = sql.Identifier(self.config['table'])
        self._entry_validator = None
        self._columns = None
        self._pm, self._pm_columns, self._pm_sql = self._ptr_map_def()
        self._primary_key = self._get_primary_key()
        if self.config['delete_db']:
            self.delete_db()
        if not (dbexists := self._db_exists()) and not self.config['create_db'] and not self.config['wait_for_db']:
            raise RuntimeError("DB does not exist, create_db is False and wait_for_db is False.")
        if not dbexists and self.config['create_db']:
            self._create_db()
        if self.config['delete_table']:
            self.delete_table()
        if not (tableexists := self._table_exists()) and not self.config['create_table'] and not self.config['wait_for_table']:
            raise RuntimeError("Table does not exist, create_table is False and wait_for_table is False.")
        self._columns = self._create_table() if not tableexists and self.config['create_table'] else self._table_definition()

    def __len__(self):
        """Return the number of entries in the table."""
        return self._db_transaction((_TABLE_LEN_SQL.format(self._table),))[0].fetchone()[0]

    def _validate_config(self):
        """Validate the table configuration."""
        if not raw_table_config_validator.validate(self.config):
            for field, error in raw_table_config_validator.errors.items():
                _logger.error(text_token({'E05000': {'error': field + ': ' + str(error)}}))
            raise ValueError
        self.config = raw_table_config_validator.sub_normalized(self.config)

    def _get_primary_key(self):
        """Identify the primary key.

        Returns
        -------
        (str) column name of the primary key or None if there is no primary key.
        """
        if 'schema' in self.config:
            for k, v in self.config['schema'].items():
                if v.get('primary_key', False):
                    return k
        return None

    def _ptr_map_def(self):
        """Pre-process the pointer map into a usable form.

        If the rows in the table define nodes in a graph then the pointer map defines
        the edges between nodes.

        self.config['ptr_map'] is of the form {
            "column X": "column Y",
            ...
        }
        where columns X contains a reference to a node identified by column Y.

        Returns
        -------
        (dict): self.config['ptr_map']
        (set): The columns used in the pointer map
        (sql.SQL): An partial SQL statement to be used in a recursive select statement.
        """
        pm_columns = set(self.config['ptr_map'].keys()) | set(self.config['ptr_map'].values())
        pm_sql = [sql.SQL('r.') + sql.Identifier(r) + sql.SQL("=t.") +
                  sql.Identifier(i) for r, i in self.config['ptr_map'].items()]
        pm_sql = sql.SQL(" OR ").join(pm_sql)
        return self.config['ptr_map'], pm_columns, pm_sql

    def _db_exists(self):
        return db_exists(self.config['database']['dbname'], self.config['database'])

    def _create_db(self):
        return db_create(self.config['database']['dbname'], self.config['database'])

    def delete_db(self):
        """Delete the database."""
        return db_delete(self.config['database']['dbname'], self.config['database'])

    def _db_transaction(self, sql_str_iter, read=True, repeatable=False):
        """Wrap db_transaction."""
        if _logit:
            for sql_str in sql_str_iter:
                _logger.debug(self._sql_to_string(sql_str))
        return db_transaction(self.config['database']['dbname'], self.config['database'], sql_str_iter, read, repeatable)

    def _sql_to_string(self, sql_str):
        """Wrap sql.SQL.as_string() to convert sql.SQL to a string (usually for logging)."""
        return sql_str.as_string(db_connect(self.config['database']['dbname'], self.config['database']))

    def _check_permissions(self):
        """Check the user has necessary permissions."""
        # TODO: One day.
        pass

    def _populate_table(self):
        """Add data to table after creation.

        Data is inserted into the table in batches of consecutive rows
        that have the same keys defined.
        This preserves order and allows columns to be set to NULL or
        their DEFAULT values.

        Only executed if this instance of raw_table() created it.
        See self._create_table().
        """
        if not len(self) and self.config['data_files']:
            for data_file in self.config['data_files']:
                abspath = join(self.config['data_file_folder'], data_file)
                _logger.info(text_token({'I05004': {'table': self.config['table'], 'file': abspath}}))
                with open(abspath, "r") as file_ptr:
                    for columns, values in self.batch_dict_data(load(file_ptr)):
                        self.insert(columns, values)

    def batch_dict_data(self, data):
        """Generate to break up an iterable of dictionaries into batches with the same keys.

        The order of dictionaries in the iterable is preserved (if it is ordered).
        Data keys that are not table columns are filtered out.

        Args
        ----
        data (iter(dict)): Each dict is a subset of a table row.

        Returns
        -------
        tuple(keys), (list(list)): A consectutive batch of rows with the same keys.
        """
        last_datum_keys = set()
        ordered_keys = tuple()
        current_batch = []
        set_of_columns = set(self._columns)
        for datum in data:
            datum_keys = set(datum.keys()) & set_of_columns
            if last_datum_keys == set(datum_keys):
                current_batch.append([datum[k] for k in ordered_keys])
            else:
                if current_batch:
                    yield ordered_keys, current_batch
                ordered_keys = tuple(datum_keys)
                current_batch = [[datum[k] for k in ordered_keys]]
                last_datum_keys = set(datum_keys)
        yield ordered_keys, current_batch

    # Get the table definition

    def _table_definition(self):
        """Get the table schema when it is defined in the database.

        Validate that the DB table has the same columns as the configuration schema if it is defined.

        Returns
        -------
        (tuple(str)): Column names.
        """
        backoff_gen = backoff_generator(
            _INITIAL_DELAY, _BACKOFF_STEPS, _BACKOFF_FUZZ)
        while not self._table_exists():
            backoff = next(backoff_gen)
            _logger.info(text_token({'I05003': {'table': self.config['table'],
                                                'dbname': self.config['database']['dbname'], 'backoff': backoff}}))
            sleep(backoff)
        results = self._db_transaction((_TABLE_DEFINITION_SQL.format(sql.Literal(self.config['table'])),))[0].fetchall()
        columns = tuple((column[0] for column in results))
        schema = {c: rtccv.normalized({'type': d.upper(), 'nullable': n == 'YES'}) for c, d, n in results}
        constraints = self._db_transaction((_TABLE_GET_PRIMARY_KEY_SQL.format(sql.Literal(self.config['table'])),))
        for column, constraint in constraints[0].fetchall():
            schema[column]['primary_key'] = constraint == 'PRIMARY KEY'
            schema[column]['unique'] = constraint == 'UNIQUE'
        self.config.setdefault('schema', schema)
        unmatched_set = set(columns) - set(self.config['schema'].keys())
        if unmatched_set:
            _logger.error(text_token({'E05001': {
                          'set': unmatched_set, 'dbname': self.config['database']['dbname'], 'table': self.config['table']}}))
            raise ValueError("Existing database table {} columns do not match configuration. Unmatched set = {}".format(
                self.config['table'], unmatched_set))
        for column in columns:
            if schema[column]['primary_key'] != self.config['schema'][column]['primary_key']:
                raise ValueError(("Existing database table {} columns do not match configuration. Column {} "
                    "PRIMARY KEY constraint is inconsistent.").format(self.config['table'], column))
            if schema[column]['unique'] != self.config['schema'][column]['unique']:
                raise ValueError(("Existing database table {} columns do not match configuration. Column {} "
                    "UNIQUE constraint is inconsistent.").format(self.config['table'], column))
            if schema[column]['nullable'] != self.config['schema'][column]['nullable']:
                raise ValueError(("Existing database table {} columns do not match configuration. Column {} "
                    "NOT NULL constraint is inconsistent.").format(self.config['table'], column))
        return columns

    def _table_exists(self):
        """Test if the table exists in the database.

        Returns
        -------
        (bool) True if the table exists else False.
        """
        return self._db_transaction((_TABLE_EXISTS_SQL.format(sql.Literal(self.config['table'])), ))[0].fetchone()[0]

    def _create_table(self):
        """Create the table if it does not exists and the user has privileges to do so.

        Assumption is that other processes may also be trying to create the table and so
        duplicate table (or privilege) exceptions are not considered errors just a race condition
        to wait out. If this process does create the table then it will populate it with any
        data specified in the configuration.

        Returns
        -------
        (tuple(str)) Column names.
        """
        columns, self._columns = [], []
        for column, definition in self.config['schema'].items():
            sql_str = " " + definition['type']
            if not definition['nullable']:
                sql_str += " NOT NULL"
            if definition['primary_key']:
                sql_str += " PRIMARY KEY"
            if definition['unique'] and not definition['primary_key']:
                sql_str += " UNIQUE"
            if 'default' in definition:
                sql_str += " DEFAULT " + definition['default']
            self._columns.append(column)
            columns.append(sql.Identifier(column) + sql.SQL(sql_str))

        sql_str = _TABLE_CREATE_SQL.format(
            self._table, sql.SQL(", ").join(columns))
        _logger.info(text_token(
            {'I05000': {'sql': self._sql_to_string(sql_str)}}))
        try:
            self._db_transaction((sql_str,), read=False)
        except ProgrammingError as e:
            if e.pgcode == errors.DuplicateTable or e.pgcode == errors.InsufficientPrivilege:
                if e.pgcode == errors.DuplicateTable:
                    _logger.info(text_token({'I05001': {
                                 'table': self.config['table'], 'dbname': self.config['database']}}))
                if e.pgcode == errors.InsufficientPrivilege:
                    _logger.info(text_token({'I05002': {
                                 'user': self.config['database']['user'], 'dbname': self.config['database']}}))
                return self._table_definition()
            raise e

        self._create_indices()
        self._populate_table()
        return self._table_definition()

    def _create_indices(self):
        """Create an index for columns that specify one."""
        for column, definition in filter(lambda x: 'index' in x[1], self.config['schema'].items()):
            sql_str = _TABLE_INDEX_SQL.format(sql.Identifier(column + "_index"), self._table)
            sql_str += sql.SQL(" USING ") + sql.Identifier(definition['index'])
            sql_str += _TABLE_INDEX_COLUMN_SQL.format(sql.Identifier(column))
            _logger.info(text_token({'I05000': {'sql': self._sql_to_string(sql_str)}}))
            self._db_transaction((sql_str,), read=False)

    def delete_table(self):
        """Delete the table."""
        sql_str = _TABLE_DELETE_TABLE_SQL.format(self._table)
        _logger.info(text_token({'I05000': {'sql': self._sql_to_string(sql_str)}}))
        self._db_transaction((sql_str,), read=False)

    def _sql_queries_transaction(self, sql_str_list, repeatable=False):
        if _logit():
            _logger.debug(text_token({'I05000': {'sql': '\n'.join([self._sql_to_string(s) for s in sql_str_list])}}))
        return tuple((dbcur.fetchall() for dbcur in self._db_transaction(sql_str_list, repeatable)))

    def select(self, query_str='', literals={}, columns='*', repeatable=False):
        """Select columns to return for rows matching query_str.

        Args
        ----
        query_str (str): Query SQL: SQL starting 'WHERE ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {one} ORDER BY {column1} ASC' where 'column1' is a column name and 'one' is a key
            in literals. If literals = {'one': 1}, columns = ('column1', 'column3') and the table name is
            'test_table' the example query_str would result in the following SQL:
                SELECT "column1", "column3" FROM "test_table" WHERE "column1" = 1 ORDER BY "column1" ASC
        literals (dict): Keys are labels used in query_str. Values are literals to replace the labels.
        columns (iter(str) or str): The columns to be returned on update if an iterable of str.
            If '*' all columns are returned. If another str interpreted as formatted SQL after 'SELECT'
            and before 'FROM' as query_str.
        repeatable (bool): If True select transaction is done with repeatable read isolation.

        Returns
        -------
        (list(tuple)): An list of the values specified by columns for the specified query_str.
        """
        if columns == '*':
            columns = self._columns
        format_dict = self._format_dict(literals)
        if isinstance(columns, str):
            columns = sql.SQL(query_str).format(**format_dict)
        else:
            columns = sql.SQL(', ').join(map(sql.Identifier, columns))
        sql_str_list = [_TABLE_SELECT_SQL.format(columns, self._table, sql.SQL(query_str).format(**format_dict))]
        return self._sql_queries_transaction(sql_str_list, repeatable)[0]

    def recursive_select(self, query_str, literals={}, columns='*', repeatable=False):
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
        columns (iter): The columns to be returned on update. If '*' defined all columns are returned.
        repeatable (bool): If True select transaction is done with repeatable read isolation.

        Returns
        -------
        (list(tuple)): An list of the values specified by columns for the specified recursive query_str
            and pointer map.
        """
        if columns == '*':
            columns = self._columns
        if not (self._pm_columns <= set(columns)):
            raise ValueError("columns must be a the same or a superset of ptr_map columns.")
        t_columns = sql.SQL('t.') + sql.SQL(', t.').join(map(sql.Identifier, columns))
        columns = sql.SQL(', ').join(map(sql.Identifier, columns))
        format_dict = self._format_dict(literals)
        sql_str_list = [_TABLE_RECURSIVE_SELECT.format(columns, self._table, sql.SQL(
            query_str).format(**format_dict), t_columns, self._pm_sql)]
        return self._sql_queries_transaction(sql_str_list, repeatable)[0]

    def _format_dict(self, literals):
        format_dict = {k: sql.Identifier(k) for k in self._columns}
        format_dict.update({k: sql.Literal(v) for k, v in literals.items()})
        return format_dict

    # TODO: This could overflow an SQL statement size limit. In which case
    # should we use a COPY https://www.postgresql.org/docs/12/dml-insert.html

    def upsert(self, columns, values, update_str=None, literals={}, returning=tuple()):
        """Upsert values.

        If update_str is None each entry will be inserted or replace the existing entry on conflict.
        In this case literals is not used.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (iter(tuple/list)): Iterable of rows (ordered iterables) with values in the order as columns.
        update_str (str): Update SQL: SQL after 'UPDATE SET ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {EXCLUDED.column1} + {one}' where 'column1' is a column name and 'one' is a key
            in literals. Prepend 'EXCLUDED.' to read the existing value. If columns = ['column1'] and
            values = [(10,)], literals = {'one': 1} and the table name is 'test_table' the example update_str
            would result in the following SQL:
                INSERT INTO "test_table" "column1" VALUES(10) ON CONFLICT DO
                    UPDATE SET "column1" = EXCLUDED."column1" + 1
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.
        returning (iter): The columns to be returned on update. If None or empty no columns will be returned.

        Returns
        -------
        (list(tuple)): An list of the values specified by returning for each updated row or [] if returning is
            an empty iterable or None.
        """
        if returning == '*':
            returning = self._columns
        if update_str is None:
            update_str = ",".join((_DEFAULT_UPDATE_STR.format(k) for k in columns if k != self._primary_key))
        if update_str != _TABLE_INSERT_CONFLICT_STR:
            if self._primary_key is None:
                raise ValueError('Can only upsert if a primary key is defined.')
            update_str = _TABLE_UPSERT_CONFLICT_STR.format('({' + self._primary_key + '})') + update_str
        columns_sql = sql.SQL(",").join([sql.Identifier(k) for k in columns])
        values_sql = sql.SQL(",").join((sql.SQL("({0})").format(
            sql.SQL(",").join((sql.Literal(value) for value in row))) for row in values))
        format_dict = self._format_dict(literals)
        format_dict.update({'EXCLUDED.' + k: sql.SQL('EXCLUDED.') + sql.Identifier(k) for k in columns})
        update_sql = sql.SQL(update_str).format(**format_dict)
        if returning:
            update_sql += _TABLE_RETURNING_SQL + sql.SQL(',').join([sql.Identifier(column) for column in returning])
        retval = self._db_transaction((_TABLE_INSERT_SQL.format(
            self._table, columns_sql, values_sql) + update_sql,), read=False)[0]
        return retval.fetchall() if returning else []

    def insert(self, columns, values):
        """Insert values.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (iter(tuple/list)): Iterable of rows (ordered iterables) with values in the order as columns.
        """
        self.upsert(columns, values, _TABLE_INSERT_CONFLICT_STR)

    def update(self, update_str, query_str, literals={}, returning=tuple()):
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
        returning (iter): An iterable of column names to return for each updated row.

        Returns
        -------
        (list(tuple)): An list of the values specified by returning for each updated row or [] if returning is
            an empty iterable or None.
        """
        if returning == '*':
            returning = self._columns
        format_dict = self._format_dict(literals)
        sql_str = _TABLE_UPDATE_SQL.format(self._table, sql.SQL(update_str).format(
            **format_dict), sql.SQL(query_str).format(**format_dict))
        if returning:
            sql_str += _TABLE_RETURNING_SQL + sql.SQL(',').join([sql.Identifier(column) for column in returning])
        retval = self._db_transaction((sql_str,), read=False)[0]
        return retval.fetchall() if returning else []

    def delete(self, query_str, literals={}, returning=tuple()):
        """Delete rows from the table.

        If query_str is not specified all rows in the table are deleted.

        Args
        ----
        query_str (str): Query SQL: SQL after 'DELETE FROM table WHERE ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {value}' where 'column1' is a column name, literals = {'value': 72}, ret=False and the table name
            is 'test_table' the example query_str would result in the following SQL:
                DELETE FROM "test_table" WHERE "column1" = 72
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.
        returning (iter): An iterable of column names to return for each deleted row.

        Returns
        -------
        (list(tuple)): An list of the values specified by returning for each updated row or [] if returning is
            an empty iterable or None.
        """
        if returning == '*':
            returning = self._columns
        format_dict = self._format_dict(literals)
        sql_str = _TABLE_DELETE_SQL.format(self._table, sql.SQL(query_str).format(**format_dict))
        if returning:
            sql_str += _TABLE_RETURNING_SQL + sql.SQL(',').join([sql.Identifier(column) for column in returning])
        retval = self._db_transaction((sql_str,), read=False)[0]
        return retval.fetchall() if returning else []
