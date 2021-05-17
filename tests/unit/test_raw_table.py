"""Unit tests for raw_table.py."""

import database
from copy import deepcopy
from psycopg2 import sql, errors, ProgrammingError, DatabaseError
from raw_table import raw_table
from utils.base_logging import get_logger


_logger = get_logger(__file__, __name__)


_SELECT_TABLE_EXISTS = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ('test_table')"
_SELECT_COLUMNS = "SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ('test_table')"
_CREATE_TABLE = "CREATE TABLE ('test_table') (('node') VARCHAR, ('id') INTEGER NOT NULL PRIMARY KEY, ('left') INTEGER, ('right') INTEGER, ('uid') INTEGER NOT NULL UNIQUE, ('meta') INTEGER [] NOT NULL)"
_TABLE_LEN = "SELECT COUNT(*) FROM ('test_table')"
_CREATE_UNIQUE_INDEX = "CREATE INDEX ('uid_index') ON ('test_table') USING 'btree'(('uid'))"
_CREATE_INDEX = "CREATE INDEX ('meta_index') ON ('test_table') USING 'btree'(('meta'))"
_SELECT = "SELECT ('uid'), ('left'), ('right') FROM ('test_table') WHERE ('test_table.id') = 7"
_RECURSIVE_SELECT = "WITH RECURSIVE rq AS (SELECT ('uid'), ('id'), ('left'), ('right') FROM ('test_table') WHERE ('test_table.id') = 7 UNION SELECT ('uid'), ('id'), ('left'), ('right') FROM ('test_table') t WHERE ('r.id')=('t.left') OR ('r.id')=('t.right')) SELECT * FROM rq"


_MOCK_CONFIG = {
    'database': {
        'dbname': 'test_db'
    },
    'table': 'test_table',
    'schema': {
        'node': {
            'type': 'VARCHAR',
            'null': True
        },
        'id': {
            'type': 'INTEGER',
            'primary_key': True
        },
        'left' : {
            'type': 'INTEGER',
            'null': True
        },
        'right' : {
            'type': 'INTEGER',
            'null': True
        },
        'uid' : {
            'type': 'INTEGER',
            'index': 'btree',
            'unique': True
        },
        'meta' : {
            'type': 'INTEGER',
            'array': True,
            'index': 'btree'
        }
    },
    'ptr_map': {
        'left': 'id',
        'right': 'id'
    },
    'format_file_folder': '../data',
    'format_file': 'data_format.json',
    'data_file_folder': '../data',
    'data_files': ['data_values.json'],
    'validate': True
}


def _MOCK_SQL_TO_STRING(self, composable):
    """Remove the need for a connection to generate a string from SQL.
    
    https://github.com/psycopg/psycopg2/issues/747
    Thanks to https://github.com/Qveshn
    """
    if isinstance(composable, sql.Composed):
        return ''.join([_MOCK_SQL_TO_STRING(None, x) for x in composable])
    elif isinstance(composable, sql.SQL):
        return composable.string
    else:
        rv = sql.ext.adapt(composable._wrapped).getquoted()
        return rv.decode() if isinstance(rv, bytes) else rv


class _MOCK_CURSOR():
    """A mock psycopg2 cursor class."""

    sql_history = []
    count = 0

    def __init__(self, sql_str_iter=tuple()):
        """Store the SQL queries and append to the history."""
        self._data = tuple(sql_str_iter)
        _MOCK_CURSOR.sql_history.extend(self._data)

    def fetchone(self):
        """Mock fetchone().

        If the SQL starts 'SELECT EXISTS (' then this is a query to see if the table exists.
        """
        if self._data[0][:15] == 'SELECT EXISTS (': return (True,)
        return self._data[0]

    def fetchall(self):
        """Mock fetchall().

        If the SQL starts 'SELECT column_n' then this is a query to get the column names.
        """
        if self._data[0][:15] == 'SELECT column_n': return tuple(([column] for column in _MOCK_CONFIG['schema'].keys()))
        return self._data


def _MOCK_DB_TRANSACTION(self, sql_str_iter, read=True, repeatable=False):
    return (_MOCK_CURSOR((_MOCK_SQL_TO_STRING(None, sql_str) for sql_str in sql_str_iter)),)


def test_invalid_config():
    """Invalidate the _MOCK_CONFIG by removing the 'table' field.

    Should raise a ValueError.
    """
    config = deepcopy(_MOCK_CONFIG)
    del config['table']
    passed = False
    try:
        rt = raw_table(config)
    except ValueError:
        passed = True
    assert passed


def test_table_exists(monkeypatch):
    """Validate a the SQL sequence when a table exists."""
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    raw_table(config)
    assert len(_MOCK_CURSOR.sql_history) == 3
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_table_does_not_exist(monkeypatch):
    """Validate a the SQL sequence when a table does not exist.
    
    In this case the table is created.
    """
    def mock_fetchone(self):
        if _MOCK_CURSOR.count == 0 and self._data[0][:15] == 'SELECT EXISTS (':
            _MOCK_CURSOR.count += 1
            return (False,)
        if self._data[0][:15] == 'SELECT COUNT(*)': return (5,)
        if self._data[0][:15] == 'SELECT EXISTS (': return (True,)
        return self._data[0]
    monkeypatch.setattr(_MOCK_CURSOR, 'fetchone', mock_fetchone)
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    raw_table(config)
    assert len(_MOCK_CURSOR.sql_history) == 7
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _CREATE_TABLE, _CREATE_UNIQUE_INDEX,
         _CREATE_INDEX, _TABLE_LEN, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_table_does_not_exist_duplicate(monkeypatch):
    """Validate a the SQL sequence when a table does not exist.
    
    In this case creation fails as the table has been created between the
    check and the create statements.
    """
    def mock_fetchone(self):
        if _MOCK_CURSOR.count == 0 and self._data[0][:15] == 'SELECT EXISTS (':
            _MOCK_CURSOR.count += 1
            return (False,)
        if self._data[0][:15] == 'SELECT COUNT(*)': return (5,)
        if self._data[0][:15] == 'SELECT EXISTS (': return (True,)
        return self._data[0]
    def mock_db_transaction(self, sql_str_iter, read=True, repeatable=False):
        sql_str_tuple = tuple((_MOCK_SQL_TO_STRING(None, sql_str) for sql_str in sql_str_iter))
        error = ProgrammingError
        error.pgcode = errors.DuplicateTable
        cursor = _MOCK_CURSOR(sql_str_tuple)
        if sql_str_tuple[0][0:13] == 'CREATE TABLE ': raise error
        return (cursor,)
    monkeypatch.setattr(_MOCK_CURSOR, 'fetchone', mock_fetchone)
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', mock_db_transaction)
    _MOCK_CURSOR.sql_history = []
    _MOCK_CURSOR.count = 0
    config = deepcopy(_MOCK_CONFIG)
    raw_table(config)
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _CREATE_TABLE, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_table_does_not_exist_privilege(monkeypatch):
    """Validate a the SQL sequence when a table does not exist.
    
    In this case creation fails as the table has been created between the
    check and the create statements.
    """
    def mock_fetchone(self):
        if _MOCK_CURSOR.count == 0 and self._data[0][:15] == 'SELECT EXISTS (':
            _MOCK_CURSOR.count += 1
            return (False,)
        if self._data[0][:15] == 'SELECT COUNT(*)': return (5,)
        if self._data[0][:15] == 'SELECT EXISTS (': return (True,)
        return self._data[0]
    def mock_db_transaction(self, sql_str_iter, read=True, repeatable=False):
        sql_str_tuple = tuple((_MOCK_SQL_TO_STRING(None, sql_str) for sql_str in sql_str_iter))
        error = ProgrammingError
        error.pgcode = errors.InsufficientPrivilege
        cursor = _MOCK_CURSOR(sql_str_tuple)
        if sql_str_tuple[0][0:13] == 'CREATE TABLE ': raise error
        return (cursor,)
    monkeypatch.setattr(_MOCK_CURSOR, 'fetchone', mock_fetchone)
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', mock_db_transaction)
    _MOCK_CURSOR.sql_history = []
    _MOCK_CURSOR.count = 0
    config = deepcopy(_MOCK_CONFIG)
    raw_table(config)
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _CREATE_TABLE, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_table_does_not_exist_other(monkeypatch):
    """Validate a the SQL sequence when a table does not exist.
    
    In this case creation fails with an unexpected exception.
    """
    def mock_fetchone(self):
        if _MOCK_CURSOR.count == 0 and self._data[0][:15] == 'SELECT EXISTS (':
            _MOCK_CURSOR.count += 1
            return (False,)
        if self._data[0][:15] == 'SELECT COUNT(*)': return (5,)
        if self._data[0][:15] == 'SELECT EXISTS (': return (True,)
        return self._data[0]
    def mock_db_transaction(self, sql_str_iter, read=True, repeatable=False):
        sql_str_tuple = tuple((_MOCK_SQL_TO_STRING(None, sql_str) for sql_str in sql_str_iter))
        cursor = _MOCK_CURSOR(sql_str_tuple)
        if sql_str_tuple[0][0:13] == 'CREATE TABLE ': raise DatabaseError
        return (cursor,)
    monkeypatch.setattr(_MOCK_CURSOR, 'fetchone', mock_fetchone)
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', mock_db_transaction)
    _MOCK_CURSOR.sql_history = []
    _MOCK_CURSOR.count = 0
    config = deepcopy(_MOCK_CONFIG)
    try: 
        raw_table(config)
    except DatabaseError:
        assert True
    else:
        assert False


def test_valid_config_with_primary_key(monkeypatch):
    """Add a primary key to the _MOCK_CONFIG and make sure we get it back."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    assert rt._primary_key == 'id'


def test_select(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.select(['WHERE {id} = {seven}'], {'seven': 7}, columns=('uid', 'left', 'right'))
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _SELECT)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_recursive_select(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.recursive_select(['WHERE {id} = {seven}'], {'seven': 7}, columns=('uid', 'left', 'right'))
    for s in _MOCK_CURSOR.sql_history: print(s)
    assert len(_MOCK_CURSOR.sql_history) == 3
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _RECURSIVE_SELECT)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str
