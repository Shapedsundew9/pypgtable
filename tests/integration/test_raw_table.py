"""Unit tests for raw_table.py."""

import database
from copy import deepcopy
from psycopg2 import sql, errors, ProgrammingError, DatabaseError
from raw_table import raw_table
from utils.base_logging import get_logger


_logger = get_logger(__file__, __name__)


_CONFIG = {
    'database': {
        'dbname': 'test_db'
    },
    'table': 'test_table',
    'schema': {
        'name': {
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
            'unique': True,
        },
        'meta' : {
            'type': 'INTEGER',
            'array': True,
            'index': 'btree',
            'null': True
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
    'validate': True,
    'create_db': True,
    'create_table': True,
    'wait_for_db': False,
    'wait_for_table': False
}


def test_create_table(monkeypatch):
    """Validate a the SQL sequence when a table exists."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    assert not rt is None
    rt.delete_table()


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
    rt.recursive_select(['WHERE {id} = {seven}'], {'seven': 7}, columns=('uid', 'id', 'left', 'right'))
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _RECURSIVE_SELECT)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_insert(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    data = (
        {"id": 91, "left":  3, "right":  4, "uid": 901, "metadata": [1, 2],    "name": "Harry"},
        {"id": 92, "left":  5, "right":  6, "uid": 902, "metadata": [],        "name": "William"},
        {"id": 93, "left":  7,              "uid": 903, "metadata": [3, 1, 2], "name": "Diana"}
    )
    rt.insert(data)
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _INSERT)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_upsert(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    data = (
        {"id": 91, "left":  3, "right":  4, "uid": 901, "metadata": [1, 2],    "name": "Harry"},
        {"id": 92, "left":  5, "right":  6, "uid": 902, "metadata": [],        "name": "William"},
        {"id": 93, "left":  7,              "uid": 903, "metadata": [3, 1, 2], "name": "Diana"}
    )
    rt.upsert(data, '{name}={EXCLUDED.name} || {temp}', {'temp': '_temp'}, ('uid', 'id'))
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _UPSERT)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_update(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    data = {"id": 1, "left": 3, "right": 4, "uid": 1, "metadata": [1, 2], "name": "Harry"}
    rt.update(data, '{name}={name} || {temp}, left={entry.left}', '{id}={entry.id}', {'temp': '_temp'}, ('uid', 'id'))
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _UPDATE)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_delete(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.delete('{id}={target}', {'target': 7}, ('uid', 'id'))
    assert len(_MOCK_CURSOR.sql_history) == 4
    for i, sql_str in enumerate((_SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _DELETE)):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_validate(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    data = (
        {"id": 91, "left":  3, "right":  4, "uid": 901, "metadata": [1, 2],    "name": "Harry"},
        {"id": 92, "left":  5, "right":  6, "uid": 902, "metadata": [],        "name": "William"},
        {"id": 93, "left":  7,              "uid": 903, "metadata": [3, 1, 2], "name": "Diana"}
    )
    results = rt.validate(data)
    assert len(results) == 3
    assert all(results)
