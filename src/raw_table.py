"""Simplified database table access."""

from logging import getLogger, DEBUG
from copy import deepcopy
from os.path import join
from json import load
from time import sleep
from psycopg2 import sql, errors, ProgrammingError
from cerberus import Validator
from cerberus.errors import ValidationError
from database import db_transaction, db_connect
from common import backoff_generator
from validators import raw_table_config_validator
from utils.text_token import text_token, register_token_code


_logger = getLogger(__name__)
_logit = lambda:_logger.getEffectiveLevel() == DEBUG


register_token_code("I05000", "SQL:\n{sql}")
register_token_code("I05001", "Table {table} cannot be created as it already exists in database {dbname}.")
register_token_code("I05002", "User {user} does not have privileges to create a table in database {dbname}.")
register_token_code("I05003", "Table {table} in database {dbname} does not yet exist. Waiting {backoff:.2}s to retry.")
register_token_code("E05000", "Configuration error: {error}")
register_token_code("E05001", "{set} columns differ between DB {dbname} and table {table} configuration.")


_INITIAL_DELAY = 0.125
_BACKOFF_STEPS = 13
_BACKOFF_FUZZ = True
_TABLE_LEN_SQL = sql.SQL("SELECT COUNT(*) FROM {0}")
_TABLE_EXISTS_SQL = sql.SQL("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {0}")
_TABLE_DEFINITION_SQL = sql.SQL("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {0}")
_TABLE_CREATE_SQL = sql.SQL("CREATE TABLE {0} ({1})")
_TABLE_INDEX_SQL = sql.SQL("CREATE INDEX {0} ON {1}")
_TABLE_INDEX_COLUMN_SQL = sql.SQL("({0})") 
_TABLE_DELETE_SQL = sql.SQL("DROP TABLE IF EXISTS {0} CASCADE")
_TABLE_RECURSIVE_SELECT = sql.SQL("WITH RECURSIVE rq AS (SELECT {0} FROM {1} {2} UNION SELECT {0} FROM {1} t WHERE {3}) SELECT * FROM rq")
_TABLE_SELECT_SQL = sql.SQL("SELECT {0} FROM {1} {2}")
_TABLE_INSERT_SQL = sql.SQL("INSERT INTO {0} ({1}) VALUES {2} ON CONFLICT ")
_TABLE_INSERT_CONFLICT_STR = " DO NOTHING"
_TABLE_UPSERT_CONFLICT_SQL = sql.SQL("DO UPDATE SET ")
_TABLE_UPDATE_SQL = sql.SQL("UPDATE {0} SET {1} WHERE {2}")
_TABLE_DELETE_SQL = sql.SQL("DELETE FROM {0} WHERE {1}")
_TABLE_RETURNING_SQL = sql.SQL(" RETURNING *")


class raw_table():
    """Connects to (or creates as needed) a postgres database & table.

    The intention of database_table is to provide a simple interface to instanciate, 
    append, update & query a persistant data store using database types (i.e. no
    automatic type conversions.)

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
        self._pm, self._pm_columns, self._pm_sql = self._ptr_map_def()
        self._primary_key = self._get_primary_key()
        self._columns = self._table_definition() if self._table_exists() else self._create_table()
    

    def __len__(self):
        """Return the number of entries in the table."""
        return self._db_transaction((_TABLE_LEN_SQL.format(self._table),))[0].fetchone()[0]


    def __getitem__(self, pk_value):
        """Query the table for the row with primary key value pk_value.
        
        Args
        ----
        pk_value (obj): A primary key value.

        Returns
        -------
        (pyscopg2.cursor) with the query results.
        """
        if self._primary_key is None: raise ValueError('SELECT row on primary key but no primary key defined!')
        return self.select(['{' + self._primary_key + '} = {_pk_value}', {'_pk_value': pk_value}]).fetchone()[0]


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
        for k, v in self.config['schema'].items():
            if v.get('primary_key', False): return k
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
        pm_columns = set(self.config['ptr_map'].keys()) | set (self.config['ptr_map'].values())
        pm_sql = [sql.Identifier('r.' + i) + sql.SQL("=") + sql.Identifier('t.' + r) for r, i in self.config['ptr_map'].items()]
        pm_sql = sql.SQL(" OR ").join(pm_sql)
        return self.config['ptr_map'], pm_columns, pm_sql


    def _db_transaction(self, sql_str_iter, read=True, repeatable=False):
        """Wrap db_transaction."""
        return db_transaction(self.config['database']['dbname'], self.config['database'], sql_str_iter, read, repeatable)


    def _sql_to_string(self, sql_str):
        """Wrap sql.SQL.as_string() to convert sql.SQL to a string (usually for logging)."""
        return sql_str.as_string(db_connect(self.config['database']['dbname'], self.config['database']))


    def _check_permissions(self):
        """Check the user has necessary permissions."""
        #TODO: One day.
        pass


    def _populate_table(self):
        """Add data to table after creation.

        Only executed if this instance of raw_table() created it.
        See self._create_table().
        """
        if not len(self) and self.config['data_files']:
            for data_file in self.config['data_files']:
                abspath = join(self.config['data_file_folder'], data_file)
                with open(abspath, "r") as datum:
                    self.insert(load(datum))
            

    # Get the table definition 
    def _table_definition(self):
        """Get the table schema when it is defined in the database.

        Validate that the DB table has the same columns as the configuration.

        Returns
        -------
        (tuple(str)): Column names.
        """ 
        backoff_gen = backoff_generator(_INITIAL_DELAY, _BACKOFF_STEPS, _BACKOFF_FUZZ)
        while not self._table_exists():
            backoff = next(backoff_gen)
            _logger.info(text_token({'I05003': {'table': self.config['table'], 
                'dbname': self.config['database']['dbname'], 'backoff': backoff}}))
            sleep(backoff)
        dbcur = self._db_transaction((_TABLE_DEFINITION_SQL.format(self._table),))[0]
        columns = tuple((column[0] for column in dbcur.fetchall()))
        unmatched_set = set(columns) - set(self.config['schema'].keys())
        #TODO: Could validate types & properties too.
        if unmatched_set:
            _logger.error(text_token({'E05001':{'set':unmatched_set, 'dbname': self.config['database']['dbname'], 'table': self.config['table']}}))
            raise ValueError("Existing database table {} columns do not match configuration. Unmatched set = {}".format(self.config['table'], unmatched_set))
        return columns


    def _table_exists(self):
        """Test if the table exists in the database.
        
        Returns
        -------
        (bool) True if the table exists else False.
        """
        return self._db_transaction((_TABLE_EXISTS_SQL.format(self._table), ))[0].fetchone()[0]


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
        columns = []
        for column, definition in self.config['schema'].items():
            sql_str = " " + definition['type']
            if definition['array']: sql_str += " []"
            if not definition['null']: sql_str += " NOT NULL"
            if definition['primary_key']: sql_str += " PRIMARY KEY"
            if definition['unique'] and not definition['primary_key']: sql_str += " UNIQUE"
            columns.append(sql.Identifier(column) + sql.SQL(sql_str))

        sql_str = _TABLE_CREATE_SQL.format(self._table, sql.SQL(", ").join(columns))
        _logger.info(text_token({'I05000': {'sql': self._sql_to_string(sql_str)}}))
        try:
            self._db_transaction((sql_str,), read=False)
        except ProgrammingError as e:
            if e.pgcode == errors.DuplicateTable or e.pgcode == errors.InsufficientPrivilege:
                if e.pgcode == errors.DuplicateTable:
                    _logger.info(text_token({'I05001': {'table': self.config['table'], 'dbname': self.config['database']}}))
                if e.pgcode == errors.InsufficientPrivilege:
                    _logger.info(text_token({'I05002': {'user': self.config['database']['user'], 'dbname': self.config['database']}}))
                return self._table_definition()
            raise e

        self._create_indices()
        self._populate_table()
        return self._table_definition()


    def _create_indices(self):
        """Create an index for columns that specify one."""
        for column, definition in filter(lambda x: 'index' in x[1], self.config['schema'].items()):
            sql_str =  _TABLE_INDEX_SQL.format(sql.Identifier(column + "_index"), self._table)
            sql_str += sql.SQL(" USING ") + sql.Literal(definition['index'])
            sql_str += _TABLE_INDEX_COLUMN_SQL.format(sql.Identifier(column))
            _logger.info(text_token({'I05000': {'sql': self._sql_to_string(sql_str)}}))
            self._db_transaction((sql_str,), read=False)


    def _delete_table(self):
        sql_str = _TABLE_DELETE_SQL.format(self._table)
        _logger.info(text_token({'I05000': {'sql': self._sql_to_string(sql_str)}}))
        self._db_transaction((sql_str,), read=False)

    
    def _sql_queries_transaction(self, sql_str_list, columns, repeatable=False):
        if _logit(): _logger.debug(text_token({'I05000': {'sql': '\n'.join([self._sql_to_string(s) for s in sql_str_list])}}))
        return (dbcur.fetchall() for dbcur in self._db_transaction(sql_str_list, repeatable))
        

    def recursive_select(self, query_strs, literals={}, columns=None, repeatable=False):
        columns = self._columns if columns is None else set(columns) | self._pm_columns
        columns = sql.SQL(', ').join(map(sql.Identifier, columns))
        format_dict = self._format_dict(literals)
        sql_str_list = [_TABLE_RECURSIVE_SELECT.format(columns, self._table, sql.SQL(q).format(**format_dict), self._pm_sql) for q in query_strs]
        return self._sql_queries_transaction(sql_str_list, columns, repeatable)


    def select(self, query_strs=['true'], literals={}, columns=None, repeatable=False):
        if columns is None: columns = self._columns
        columns = sql.SQL(', ').join(map(sql.Identifier, columns))
        format_dict = self._format_dict(literals)
        sql_str_list = [_TABLE_SELECT_SQL.format(columns, self._table, sql.SQL(q).format(**format_dict)) for q in query_strs]
        return self._sql_queries_transaction(sql_str_list, columns, repeatable)


    def insert(self, entries):
        return self.upsert(entries, _TABLE_INSERT_CONFLICT_STR)


    def _format_dict(self, literals):
        format_dict = {k: sql.Identifier(self.config['table'] + '.' + k) for k in self._columns}
        format_dict.update({k: sql.Literal(v) for k, v in literals.items()})
        return format_dict


    # TODO: This could overflow an SQL statement size limit. In which case
    # should we use a COPY https://www.postgresql.org/docs/12/dml-insert.html
    def upsert(self, entries, update_str=None, literals={}):
        """Upsert entries.

        If update_str is None each entry will be inserted or replace the existing entry on conflict.
        In this case literals is not used.

        Args
        ----
        entries (list(dict)): Keys are column names. Values are values to insert or use in update.
        update_str (str): Update SQL: SQL after 'UPDATE SET ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {EXCLUDED.column1} + {one}' where 'column1' is a column name and 'one' is a key
            in literals. The table identifier will be appended to any column names. Prepend 'EXCLUDED.' to read
            the existing value. If entries = [{'column1': 'example'}], literals = {'one': 1} and the table name
            is 'test_table' the example update_str would result in the following SQL:
                INSERT INTO "test_table" ("column1") VALUES('example') ON CONFLICT DO 
                    UPDATE SET "test_table.column1" = "EXCLUDED.column1" + 1
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.

        Returns
        -------
        (psycopg2.cursor or None)
        """
        if not entries: return None
        if update_str is None: update_str = ",".join((k + '=' + k for k in entries[0].keys() if k != self._primary_key))
        if update_str != _TABLE_INSERT_CONFLICT_STR: update_str = _TABLE_UPSERT_CONFLICT_SQL + update_str
        columns = sql.SQL(",").join([sql.Identifier(k) for k in entries[0].keys()])
        values = sql.SQL(",").join((sql.SQL("({0})").format(sql.SQL(",").join((sql.Literal(v) for v in e.values()))) for e in entries))
        format_dict = self._format_dict(literals)
        format_dict.update({'EXCLUDED.' + k: sql.Identifier('EXCLUDED.' + k) for k in entries[0].keys()})
        update_sql = sql.SQL(update_str).format(**format_dict)
        return self._db_transaction((_TABLE_INSERT_SQL.format(self._table, columns, values) + update_sql,), read=False)[0]


    def update(self, entries, update_str=None, query_str=['True'], literals={}, ret=False):
        """Update entries.

        If update_str is None each entry will be inserted or replace the existing entry on conflict.
        In this case literals is not used.

        Args
        ----
        entries (list(dict)): Keys are column names. Values are values to insert or use in update.
        update_str (str): Update SQL: SQL after 'UPDATE SET ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {EXCLUDED.column1} + {one}' where 'column1' is a column name and 'one' is a key
            in literals. The table identifier will be appended to any column names. Prepend 'EXCLUDED.' to read
            the existing value. If entries = [{'column1': 'example'}], literals = {'one': 1} and the table name
            is 'test_table' the example update_str would result in the following SQL:
                INSERT INTO "test_table" ("column1") VALUES('example') ON CONFLICT DO 
                    UPDATE SET "test_table.column1" = "EXCLUDED.column1" + 1
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.

        Returns
        -------
        (psycopg2.cursor or None)
        """
        if not entries: return None
        if update_str is None: update_str = ",".join((k + '=' + k for k in entries[0].keys() if k != self._primary_key))
        format_dict = self._format_dict(literals)
        sql_str = _TABLE_UPDATE_SQL.format(self._table, sql.SQL(update_str).format(**format_dict), sql.SQL(query_str).format(**format_dict))
        if ret: sql_str += _TABLE_RETURNING_SQL
        return self._db_transaction((sql_str,), read=False)[0]


    def delete(self, query_str=['true'], literals={}, ret=False):
        """Delete rows from the table.

        If query_str is not specified all rows in the table are deleted.

        Args
        ----
        query_str (str): Query SQL: SQL after 'DELETE FROM table WHERE ' using '{column/literal}' for identifiers/literals.
            e.g. '{column1} = {value}' where 'column1' is a column name, literals = {'value': 72}, ret=False and the table name
            is 'test_table' the example query_str would result in the following SQL:
                DELETE FROM "test_table" WHERE "column1" = 72
        literals (dict): Keys are labels used in update_str. Values are literals to replace the labels.

        Returns
        -------
        (psycopg2.cursor)
        """
        format_dict = self._format_dict(self._columns, literals)
        sql_str = _TABLE_DELETE_SQL.format(self._table, sql.SQL(query_str).format(**format_dict))
        if ret: sql_str += _TABLE_RETURNING_SQL
        return self._db_transaction((sql_str,), read=False)[0]


    def validate(self, entries):
        """Validate entries.

        Entries are automatically validated (return True for all) if the table configuration
        does not have validate set.

        Args
        ----
        entries (list(dict)): List of entries to validate.

        Returns
        -------
        (tuple(bool)): Tuple with the same length as entries with the validity of each entry.
        """
        if self.config['validate']:
            if self._entry_validator is None:
                schema_path = join(self.config['format_file_folder'], self.config['format_file'])
                with open(schema_path, "r") as schema_file:
                    self._entry_validator = Validator(load(schema_file))
            return tuple((self._entry_validator(entry) for entry in entries))
        return (True,) * len(entries)

