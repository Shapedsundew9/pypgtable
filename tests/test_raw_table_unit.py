"""Unit tests for raw_table.py."""

from copy import deepcopy
from os.path import join, dirname
from psycopg2 import sql, errors, ProgrammingError, DatabaseError
from pypgtable.raw_table import raw_table
from pypgtable.utils.base_logging import get_logger


_logger = get_logger(__file__, __name__)


# Expected 'SQL' statements
_DROP_TABLE = "DROP TABLE IF EXISTS ('test_table') CASCADE"
_SELECT_TABLE_EXISTS = ("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = "
    "'public' AND table_name = 'test_table')")
_SELECT_COLUMNS = ("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = "
    "'public' AND table_name = 'test_table'")
_CREATE_TABLE = ("CREATE TABLE ('test_table') (('name') VARCHAR, ('id') INTEGER NOT NULL PRIMARY KEY, ('left') "
    "INTEGER, ('right') INTEGER, ('uid') INTEGER NOT NULL UNIQUE, ('updated') TIMESTAMP NOT NULL DEFAULT NOW(), "
    "('metadata') INTEGER [])")
_TABLE_LEN = "SELECT COUNT(*) FROM ('test_table')"
_CREATE_UNIQUE_INDEX = "CREATE INDEX ('uid_index') ON ('test_table') USING ('btree')(('uid'))"
_CREATE_INDEX = "CREATE INDEX ('metadata_index') ON ('test_table') USING ('btree')(('metadata'))"
_SELECT = "SELECT ('uid'), ('left'), ('right') FROM ('test_table') WHERE ('id') = 7"
_RECURSIVE_SELECT = ("WITH RECURSIVE rq AS (SELECT ('uid'), ('id'), ('left'), ('right') FROM ('test_table') "
    "WHERE ('id') = 7 UNION SELECT t.('uid'), t.('id'), t.('left'), t.('right') FROM ('test_table') t INNER "
    "JOIN rq r ON r.('left')=t.('id') OR r.('right')=t.('id')) SELECT * FROM rq")
_INSERT = ("INSERT INTO ('test_table') (('id'),('left'),('right'),('uid'),('metadata'),('name')) VALUES "
    "(91,3,4,901,ARRAY[1,2],'Harry'),(92,5,6,902,'{}','William') ON CONFLICT DO NOTHING")
_UPSERT = ("INSERT INTO ('test_table') (('id'),('left'),('right'),('uid'),('metadata'),('name')) VALUES "
    "(91,3,4,901,ARRAY[1,2],'Harry'),(0,1,2,201,'{}','Diana') ON CONFLICT (('id')) DO UPDATE SET "
    "('name')=EXCLUDED.('name') || '_temp' RETURNING ('uid'),('id'),('name')")
_UPDATE = ("UPDATE ('test_table') SET ('name')=('name') || '_new' WHERE ('id')=0 RETURNING ('id'),('name')")
_DELETE = "DELETE FROM ('test_table') WHERE ('id')=7 RETURNING ('uid'),('id')"


# Expected groups of 'SQL' statements
_TABLE_EXISTS_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)
_TABLE_DOES_NOT_EXIST_EXPECTED = (_DROP_TABLE,_SELECT_TABLE_EXISTS, _CREATE_TABLE, _CREATE_UNIQUE_INDEX,
         _CREATE_INDEX, _TABLE_LEN, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)
_TABLE_DUPLICATE_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _CREATE_TABLE, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS)
_TABLE_PRIVILEGE_EXPECTED = _TABLE_DUPLICATE_EXPECTED
_SELECT_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _SELECT)
_RECURSIVE_SELECT_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _RECURSIVE_SELECT)
_INSERT_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _INSERT)
_UPSERT_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _UPSERT)
_UPDATE_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _UPDATE)
_DELETE_EXPECTED = (_DROP_TABLE, _SELECT_TABLE_EXISTS, _SELECT_TABLE_EXISTS, _SELECT_COLUMNS, _DELETE)


_MOCK_CONFIG = {
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
        'updated': {
            'type': 'TIMESTAMP',
            'default': 'NOW()'
        },
        'metadata' : {
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
    'format_file_folder': join(dirname(__file__), 'data'),
    'format_file': 'data_format.json',
    'data_file_folder': join(dirname(__file__), 'data'),
    'data_files': ['data_values.json'],
    'validate': True,
    'delete_db': False,
    'delete_table': True,
    'create_db': True,
    'create_table': True,
    'wait_for_db': False,
    'wait_for_table': False
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
        for s in self._data: print(s)

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
        if self._data[0][:15] == 'SELECT column_n': return sorted(([column] for column in _MOCK_CONFIG['schema'].keys()))
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
    assert len(_MOCK_CURSOR.sql_history) == len(_TABLE_EXISTS_EXPECTED)
    for i, sql_str in enumerate(_TABLE_EXISTS_EXPECTED):
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
    assert len(_MOCK_CURSOR.sql_history) == len(_TABLE_DOES_NOT_EXIST_EXPECTED)
    for i, sql_str in enumerate(_TABLE_DOES_NOT_EXIST_EXPECTED):
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
    assert len(_MOCK_CURSOR.sql_history) == len(_TABLE_DUPLICATE_EXPECTED)
    for i, sql_str in enumerate(_TABLE_DUPLICATE_EXPECTED):
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
    assert len(_MOCK_CURSOR.sql_history) == len(_TABLE_PRIVILEGE_EXPECTED)
    for i, sql_str in enumerate(_TABLE_PRIVILEGE_EXPECTED):
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
    rt.select('WHERE {id} = {seven}', {'seven': 7}, columns=('uid', 'left', 'right'))
    assert len(_MOCK_CURSOR.sql_history) == len(_SELECT_EXPECTED)
    for i, sql_str in enumerate(_SELECT_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_recursive_select(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.recursive_select('WHERE {id} = {seven}', {'seven': 7}, columns=('uid', 'id', 'left', 'right'))
    assert len(_MOCK_CURSOR.sql_history) == len(_RECURSIVE_SELECT_EXPECTED)
    for i, sql_str in enumerate(_RECURSIVE_SELECT_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_insert(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (92, 5, 6, 902, [], "William"))
    rt.insert(columns, values)
    assert len(_MOCK_CURSOR.sql_history) == len(_INSERT_EXPECTED)
    for i, sql_str in enumerate(_INSERT_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_upsert(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (0, 1, 2, 201, [], "Diana"))
    rt.upsert(columns, values, '{name}={EXCLUDED.name} || {temp}', {'temp': '_temp'}, ('uid', 'id', 'name'))
    assert len(_MOCK_CURSOR.sql_history) == len(_UPSERT_EXPECTED)
    for i, sql_str in enumerate(_UPSERT_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_update(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.update('{name}={name} || {new}', '{id}={qid}', {'qid':0, 'new': '_new'}, ('id', 'name'))
    assert len(_MOCK_CURSOR.sql_history) == len(_UPDATE_EXPECTED)
    for i, sql_str in enumerate(_UPDATE_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_delete(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    rt.delete('{id}={target}', {'target': 7}, ('uid', 'id'))
    assert len(_MOCK_CURSOR.sql_history) == len(_DELETE_EXPECTED)
    for i, sql_str in enumerate(_DELETE_EXPECTED):
        assert _MOCK_CURSOR.sql_history[i] == sql_str


def test_validate(monkeypatch):
    """As it says on the tin."""
    monkeypatch.setattr(raw_table, '_sql_to_string', _MOCK_SQL_TO_STRING)
    monkeypatch.setattr(raw_table, '_db_transaction', _MOCK_DB_TRANSACTION)
    _MOCK_CURSOR.sql_history = []
    config = deepcopy(_MOCK_CONFIG)
    rt = raw_table(config)
    columns = ('id', 'left', 'right', 'uid', 'metadata', 'name')
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (92, 5, 6, 902, [], "William"))
    results = rt.validate(columns, values)
    assert len(results) == len(values)
    assert all(results)
