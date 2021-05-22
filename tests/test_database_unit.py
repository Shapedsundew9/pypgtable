"""Unit tests for the database.py module."""

from psycopg2 import sql, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_DEFAULT, ISOLATION_LEVEL_REPEATABLE_READ
from pypgtable import database
from pypgtable.utils.reference import sequential_reference
from pypgtable.database import _connect_core, db_reconnect, db_connect, db_disconnect, db_transaction
from pypgtable.database import db_disconnect_all, _DB_TRANSACTION_ATTEMPTS, db_exists, db_create, db_delete
from pypgtable.common import backoff_generator
from pypgtable.utils.base_logging import get_logger


_logger = get_logger(__file__, __name__)


_MOCK_CONFIG = {
    'host': '_host',
    'port': '_port',
    'user': '_user',
    'password': '_password',
    'maintenance_db': '_maintenance_db',
    'retries': 100000}
_MOCK_DBNAME = '_dbname'
_MOCK_VALUE_1 = 1234
_MOCK_VALUE_2 = 4321
_MOCK_ERROR = 0
_INFINITE_BACKOFFS = 100


def test_connect_core_p0(monkeypatch):
    """Positive path for _connection_core()."""
    db_disconnect_all()

    class mock_connection():
        def __init__(self) -> None: self.value = _MOCK_VALUE_1
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert _connect_core(_MOCK_DBNAME, _MOCK_CONFIG)[0].value == _MOCK_VALUE_1


def test_connect_core_n0(monkeypatch):
    """Raise an OperationalError in _connection_core()."""
    db_disconnect_all()

    class mock_connection():
        def __init__(self) -> None: raise OperationalError
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert _connect_core(_MOCK_DBNAME, _MOCK_CONFIG)[0] is None


def test_db_reconnect_p0(monkeypatch):
    """Reconnect to the DB with no initial connection."""
    db_disconnect_all()

    class mock_connection():
        def __init__(self) -> None: self.value = _MOCK_VALUE_1
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert db_reconnect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_1


def test_db_reconnect_p1(monkeypatch):
    """Reconnect to the DB with a pre-existing connection."""
    db_disconnect_all()

    def mock_values_iter():
        yield _MOCK_VALUE_1
        yield _MOCK_VALUE_2
    mock_values = mock_values_iter()

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_values)
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setitem(database._connections, _MOCK_CONFIG['host'], {_MOCK_DBNAME: mock_connection()})
    assert db_reconnect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2


def test_db_reconnect_n0(monkeypatch):
    """Pre-existing connection close() raises an OperationalError."""
    db_disconnect_all()

    def mock_values_iter():
        yield _MOCK_VALUE_1
        yield _MOCK_VALUE_2
    mock_values = mock_values_iter()

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_values)
        def close(self): raise OperationalError

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setitem(database._connections, _MOCK_CONFIG['host'], {_MOCK_DBNAME: mock_connection()})
    assert db_reconnect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2


def test_db_reconnect_n1(monkeypatch):
    """Connection raises OperationalError.

    There is a pre-existing connection.
    The pre-existing connection is successfully closed.
    The 1st reconnection attempt raises an OperationalError.
    The 2nd reconnection attempt succeeds.

    db_reconnect should return the second successful connection after
    one backoff.
    """
    db_disconnect_all()

    def _connection_iter():
        for i in (_MOCK_VALUE_1, _MOCK_ERROR, _MOCK_VALUE_2):
            yield i
    mock_values = _connection_iter()
    global sleep_duration
    sleep_duration = 0.0

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_values)
            if self.value == _MOCK_ERROR:
                raise OperationalError

        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()

    def mock_sleep(backoff):
        global sleep_duration
        sleep_duration += backoff
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(database, 'sleep', mock_sleep)
    backoff = next(backoff_generator(fuzz=False))
    monkeypatch.setitem(database._connections, _MOCK_CONFIG['host'], {_MOCK_DBNAME: mock_connection()})
    assert db_reconnect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2
    assert backoff >= sleep_duration/1.1 and backoff <= sleep_duration/0.9


def test_db_reconnect_n2(monkeypatch):
    """Check infinite backoff.

    There is a pre-existing connection.
    The pre-existing connection is successfully closed.
    The _INFINITE_BACKOFFS reconnection attempts raises an OperationalError.
    The _INFINITE_BACKOFFS+1 reconnection attempt succeeds.

    db_reconnect should return the second successful connection after
    _INFINITE_BACKOFFS backoffs.
    """
    db_disconnect_all()

    def _connection_iter():
        connections = [_MOCK_VALUE_1]
        connections.extend([_MOCK_ERROR] * _INFINITE_BACKOFFS)
        connections.append(_MOCK_VALUE_2)
        for i in connections:
            yield i
    mock_values = _connection_iter()
    global sleep_duration
    sleep_duration = 0.0

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_values)
            if self.value == _MOCK_ERROR:
                raise OperationalError

        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()

    def mock_sleep(backoff):
        global sleep_duration
        sleep_duration += backoff
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(database, 'sleep', mock_sleep)
    monkeypatch.setitem(database._connections, _MOCK_CONFIG['host'], {_MOCK_DBNAME: mock_connection()})
    backoff_gen = backoff_generator(fuzz=False)
    total_backoff = sum((next(backoff_gen) for _ in range(_INFINITE_BACKOFFS)))
    assert db_reconnect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2
    assert total_backoff >= sleep_duration / 1.1 and total_backoff <= sleep_duration / 0.9


def test_db_connect_p0(monkeypatch):
    """No pre-existing connection test for db_connect()."""
    db_disconnect_all()

    class mock_connection():
        def __init__(self) -> None: self.value = _MOCK_VALUE_1
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_1


def test_db_connect_p1(monkeypatch):
    """With pre-existing connection test for db_connect()."""
    db_disconnect_all()

    def mock_values_iter():
        yield _MOCK_VALUE_1
        yield _MOCK_VALUE_2
    mock_values = mock_values_iter()

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_values)
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_1
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_1


def test_db_disconnect_p0(monkeypatch):
    """Create a connection and then disconnect it.

    Connection should be closed.
    A new connection should be a new connection object.
    """
    db_disconnect_all()

    def mock_values_iter():
        yield _MOCK_VALUE_1
        yield _MOCK_VALUE_2
    mock_values = mock_values_iter()

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_values)
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    connection = db_connect(_MOCK_DBNAME, _MOCK_CONFIG)
    assert connection.value == _MOCK_VALUE_1
    db_disconnect(_MOCK_DBNAME, _MOCK_CONFIG)
    assert connection.value is None
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2


def test_db_disconnect_n0(monkeypatch):
    """Create a connection and then disconnect it with an OperationalError on close().

    A new connection should be a new connection object.
    """
    db_disconnect_all()

    def mock_values_iter():
        yield _MOCK_VALUE_1
        yield _MOCK_VALUE_2
    mock_values = mock_values_iter()

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_values)
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_1
    db_disconnect(_MOCK_DBNAME, _MOCK_CONFIG)
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).value == _MOCK_VALUE_2


def test_db_transaction_p0(monkeypatch):
    """Execute a read-only SQL statement.

    A single cursor should be returned.
    """
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", ))
    assert len(dbcur_list) == 1
    assert not dbcur_list[0].fetchone()


def test_db_transaction_p1(monkeypatch):
    """Execute multiple read-only SQL statements.

    A cursor for each SQL statement should be returned in the order
    the statement were submitted.
    """
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"))
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == 0
    assert dbcur_list[1].fetchone() == 1
    assert dbcur_list[2].fetchone() == 2


def test_db_transaction_p2(monkeypatch):
    """Test that the isolation level is correctly set and cleared."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_connection_ref)
            self.isolation_level = ISOLATION_LEVEL_DEFAULT

        def close(self): self.value = None

        def cursor(self):
            assert self.isolation_level == ISOLATION_LEVEL_REPEATABLE_READ
            return mock_cursor()

        def set_session(
            self, isolation_level): self.isolation_level = isolation_level

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"), repeatable=True)
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == 0
    assert dbcur_list[1].fetchone() == 1
    assert dbcur_list[2].fetchone() == 2
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).isolation_level == ISOLATION_LEVEL_DEFAULT


def test_db_transaction_p3(monkeypatch):
    """Test that a write transaction is committed."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_connection_ref)
            self.committed = False

        def close(self): self.value = None

        def cursor(self):
            self.committed = False
            return mock_cursor()

        def commit(self): self.committed = True

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(
        _MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"), read=False)
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == 0
    assert dbcur_list[1].fetchone() == 1
    assert dbcur_list[2].fetchone() == 2
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).commit


def test_db_transaction_n0(monkeypatch):
    """Raise an OperationalError on the 2nd of 3 read SQL statements.

    A cursor for each SQL statement should be returned in the order
    the statement were submitted.

    0. The first statement execution will be discarded
    1. The second statement execution will produce no results (OperationalError)
    2. The first statement will be re-executed
    3. The second statement will be re-executed
    4. The third statement will be executed

    Should get 3 cursors with the values 2, 3 & 4
    """
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)

        def execute(self, sql_str):
            if self.value == 1:
                raise OperationalError

        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"))
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == 2
    assert dbcur_list[1].fetchone() == 3
    assert dbcur_list[2].fetchone() == 4


def test_db_transaction_n1(monkeypatch):
    """Raise _DB_TRANSACTION_ATTEMPTS OperationalErrors to force a reconnection.

    A cursor for each SQL statement should be returned in the order
    the statement were submitted.

    0. The first statement execution produce no results _DB_TRANSACTION_ATTEMPTS times (OperationalError)
    1. A reconnection will occur
    2. The first statement will be re-executed
    3. The second statement will be executed
    4. The third statement will be executed

    Should get 3 cursors with the values _DB_TRANSACTION_ATTEMPTS, _DB_TRANSACTION_ATTEMPTS+1, & _DB_TRANSACTION_ATTEMPTS+2
    The next mock_connection_ref should be 2
    """
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)

        def execute(self, sql_str):
            if self.value < _DB_TRANSACTION_ATTEMPTS:
                raise OperationalError

        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"))
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == _DB_TRANSACTION_ATTEMPTS
    assert dbcur_list[1].fetchone() == _DB_TRANSACTION_ATTEMPTS + 1
    assert dbcur_list[2].fetchone() == _DB_TRANSACTION_ATTEMPTS + 2
    assert next(mock_connection_ref) == 2


def test_db_transaction_n2(monkeypatch):
    """Test that a write transaction with an error is rolled back & re-attempted."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)

        def execute(self, sql_str):
            if self.value == 1:
                raise OperationalError

        def fetchone(self): return self.value

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_connection_ref)
            self.isolation_level = ISOLATION_LEVEL_DEFAULT
            self.committed = False
            self.rolledback = False

        def close(self): self.value = None
        def cursor(self): return mock_cursor()
        def commit(self): self.committed = True
        def rollback(self): self.rolledback = True

    def mock_connect(*args, **kwargs): return mock_connection()
    monkeypatch.setattr(database, 'connect', mock_connect)
    dbcur_list = db_transaction(_MOCK_DBNAME, _MOCK_CONFIG, ("SQL0", "SQL1", "SQL2"), read=False)
    assert len(dbcur_list) == 3
    assert dbcur_list[0].fetchone() == 2
    assert dbcur_list[1].fetchone() == 3
    assert dbcur_list[2].fetchone() == 4
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).commit
    assert db_connect(_MOCK_DBNAME, _MOCK_CONFIG).rolledback


def test_db_exists_p0(monkeypatch):
    """Test the case when the DB exists."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchall(self): return ((_MOCK_DBNAME,),)

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    def mock_as_string(*args, **kwargs): return "SQL string"
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(sql.SQL, 'as_string', mock_as_string)
    assert db_exists(_MOCK_DBNAME, _MOCK_CONFIG)


def test_db_exists_p1(monkeypatch):
    """Test the case when the DB does not exist."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass
        def fetchall(self): return ((_MOCK_DBNAME,),)

    class mock_connection():
        def __init__(self) -> None: self.value = next(mock_connection_ref)
        def cursor(self): return mock_cursor()
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    def mock_as_string(*args, **kwargs): return "SQL string"
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(sql.SQL, 'as_string', mock_as_string)
    assert not db_exists("Does not exist", _MOCK_CONFIG)


def test_db_create_p0(monkeypatch):
    """Create a DB."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_connection_ref)
            self.autocommit = False

        def cursor(self): return mock_cursor()
        def commit(self): pass
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    def mock_as_string(*args, **kwargs): return "SQL string"
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(sql.Composed, 'as_string', mock_as_string)
    db_create(_MOCK_DBNAME, _MOCK_CONFIG)


def test_db_delete_p0(monkeypatch):
    """Delete a DB."""
    db_disconnect_all()
    mock_connection_ref = sequential_reference()
    mock_cursor_ref = sequential_reference()

    class mock_cursor():
        def __init__(self) -> None: self.value = next(mock_cursor_ref)
        def execute(self, sql_str): pass

    class mock_connection():
        def __init__(self) -> None:
            self.value = next(mock_connection_ref)
            self.autocommit = False

        def cursor(self): return mock_cursor()
        def commit(self): pass
        def close(self): self.value = None

    def mock_connect(*args, **kwargs): return mock_connection()
    def mock_as_string(*args, **kwargs): return "SQL string"
    monkeypatch.setattr(database, 'connect', mock_connect)
    monkeypatch.setattr(sql.Composed, 'as_string', mock_as_string)
    db_delete(_MOCK_DBNAME, _MOCK_CONFIG)
