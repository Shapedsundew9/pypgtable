"""Unit tests for raw_table.py."""

from copy import deepcopy
from inspect import stack
from json import load
from logging import NullHandler, getLogger
from os.path import dirname, join

from pytest import approx

from pypgtable import table

_logger = getLogger(__name__)
_logger.addHandler(NullHandler())


_CONFIG = {
    "database": {"dbname": "test_db"},
    "table": "test_table",
    "schema": {
        "name": {"type": "VARCHAR", "nullable": True},
        "id": {"type": "INTEGER", "primary_key": True},
        "left": {"type": "INTEGER", "nullable": True},
        "right": {"type": "INTEGER", "nullable": True},
        "uid": {
            "type": "INTEGER",
            "index": "btree",
            "unique": True,
        },
        "updated": {"type": "TIMESTAMP", "default": "NOW()"},
        "metadata": {"type": "INTEGER[]", "index": "btree", "nullable": True},
    },
    "ptr_map": {"left": "id", "right": "id"},
    "data_file_folder": join(dirname(__file__), "data"),
    "data_files": ["data_values.json"],
    "delete_db": False,
    "delete_table": True,
    "create_db": True,
    "create_table": True,
    "wait_for_db": False,
    "wait_for_table": False,
}


with open(join(dirname(__file__), "data/data_values.json"), "r") as fileptr:
    _DEFAULT_TABLE_LENGTH = len(load(fileptr))


def _register_conversions(table):
    table.register_conversion("id", lambda x: x - 1000, lambda x: x + 1000)
    table.register_conversion("name", lambda x: x.lower(), lambda x: x.upper())
    return table


def test_create_table():
    """Validate a the SQL sequence when a table exists."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    assert t is not None


def test_len():
    """Make sure the table length is returned."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    rt = table(config)
    assert len(rt) == _DEFAULT_TABLE_LENGTH


def test_getitem_encoded_pk1():
    """Validate a valid getitem for an encoded primary key."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    expected = {
        "id": 1000,
        "left": 1,
        "right": 2,
        "uid": 100,
        "metadata": None,
        "name": "ROOT",
    }
    result = t[1000]
    assert all([expected[k] == result[k] for k in expected])


def test_getitem_encoded_pk2():
    """Validate an invalid getitem for an encoded primary key."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    try:
        t[0]
    except KeyError:
        pass
    else:
        assert False


def test_getitem_pk1():
    """Validate a valid getitem."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    expected = {
        "id": 0,
        "left": 1,
        "right": 2,
        "uid": 100,
        "metadata": None,
        "name": "root",
    }
    result = t[0]
    assert all([expected[k] == result[k] for k in expected])


def test_getitem_pk2():
    """Validate an invalid getitem."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    try:
        t[1000]
    except KeyError:
        pass
    else:
        assert False


def test_getitem_no_pk():
    """Validate if the table has no primary key we get the correct ValueError."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    config["schema"]["id"]["primary_key"] = False
    t = table(config)
    try:
        t[0]
    except ValueError as e:
        assert str(e) == "SELECT row on primary key but no primary key defined!"
    else:
        assert False


def test_setitem_encoded_pk():
    """Validate a valid setitem for an encoded primary key."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    setitem = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    expected_decoded = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "ROOT",
    }
    expected_raw = {
        "id": -978,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "root",
    }
    t[22] = setitem
    result = t[22]
    raw_result = dict(zip(t.raw.columns, next(t.raw.select("WHERE {id} = -978"))))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])


def test_setitem_pk():
    """Validate a valid setitem."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    setitem = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    expected_decoded = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    expected_raw = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    t[22] = setitem
    result = t[22]
    raw_result = dict(zip(t.raw.columns, next(t.raw.select("WHERE {id} = 22"))))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])


def test_setitem_mismatch_pk():
    """When setting an item and specifying the primary key in the value the setitem key takes precedence."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    setitem = {
        "id": 22,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    expected_decoded = {
        "id": 28,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    expected_raw = {
        "id": 28,
        "left": 9,
        "right": 12,
        "uid": 122,
        "metadata": [34, 78],
        "name": "rOoT",
    }
    t[28] = setitem
    result = t[28]
    raw_result = dict(zip(t.raw.columns, next(t.raw.select("WHERE {id} = 28"))))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])
    try:
        t[22]
    except KeyError:
        pass
    else:
        assert False


def test_select_tuple():
    """Validate select returning a tuple."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = list(
        t.select(
            "WHERE {id} = {seven}",
            {"seven": 7},
            columns=("uid", "left", "right"),
            container="tuple",
        )
    )
    assert data == [(107, 13, None)]


def test_select_dict():
    """Validate select returning a dict."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = list(
        t.select(
            "WHERE {id} = {seven}",
            {"seven": 7},
            columns=("uid", "left", "right"),
            container="dict",
        )
    )
    assert data == [{"uid": 107, "left": 13, "right": None}]


def test_select_all_columns():
    """Validate select returning all columns using '*' (the default)."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = tuple(t.select(container="tuple"))
    assert len(data[0]) == len(t.columns())


def test_recursive_select():
    """Validate a recursive select returning a tuple."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = list(t.recursive_select("WHERE {id} = 2", columns=("id", "uid", "left", "right"), container="tuple"))
    assert data == [
        (2, 102, 5, 6),
        (5, 105, 10, 11),
        (6, 106, None, 12),
        (10, 110, None, None),
        (11, 111, None, None),
        (12, 112, None, None),
    ]


def test_recursive_select_no_pk():
    """Validate a recursive select returning a pkdict without specifying the primary key."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = tuple(t.recursive_select("WHERE {id} = 2", columns=("uid", "left", "right"), container="pkdict"))
    assert len(data)


def test_upsert():
    """Validate an upsert consisting or 1 insert & 1 update returing updated fields as tuples."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = (
        {
            "id": 91,
            "left": 3,
            "right": 4,
            "uid": 901,
            "metadata": [1, 2],
            "name": "Harry",
        },
        {"id": 0, "left": 1, "right": 2, "uid": 201, "metadata": [], "name": "Diana"},
    )
    returning = t.upsert(
        data,
        "{name}={EXCLUDED.name} || {temp}",
        {"temp": "_temp"},
        ("uid", "id", "name"),
        container="tuple",
    )
    row = t.select(
        "WHERE {id} = 0",
        columns=("id", "left", "right", "uid", "metadata", "name"),
        container="tuple",
    )
    assert list(returning) == [(901, 91, "Harry"), (100, 0, "Diana_temp")]
    assert list(row) == [(0, 1, 2, 100, None, "Diana_temp")]


def test_upsert_no_pk():
    """Validate an upsert returning a pkdict without specifying the primary key."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    data = (
        {
            "id": 91,
            "left": 3,
            "right": 4,
            "uid": 901,
            "metadata": [1, 2],
            "name": "Harry",
        },
        {"id": 0, "left": 1, "right": 2, "uid": 201, "metadata": [], "name": "Diana"},
    )
    returning = tuple(
        t.upsert(
            data,
            "{name}={EXCLUDED.name} || {temp}",
            {"temp": "_temp"},
            ("uid", "name"),
            container="invalid_so_dict",
        )
    )
    row = list(
        t.select(
            "WHERE {id} = 0",
            columns=("id", "left", "right", "uid", "metadata", "name"),
            container="tuple",
        )
    )
    assert len(returning) == 2 and isinstance(returning[0], dict)
    assert row == [(0, 1, 2, 100, None, "Diana_temp")]


def test_insert():
    """Validate inserting two rows from a dict."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    data = [
        {
            "id": 91,
            "left": 3,
            "right": 4,
            "uid": 901,
            "metadata": [1, 2],
            "name": "Harry",
        },
        {
            "id": 92,
            "left": 5,
            "right": 6,
            "uid": 902,
            "metadata": [],
            "name": "William",
        },
    ]
    t.insert(data)
    results = list(t.select("WHERE {id} > 90 ORDER BY {id} ASC", columns=columns))
    assert data == results


def test_update():
    """Validate an update returning a dict."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = t.update(
        "{name}={name} || {new}",
        "{id}={qid}",
        {"qid": 0, "new": "_new"},
        ("id", "name"),
        container="dict",
    )
    row = t.select(
        "WHERE {id} = 0",
        columns=("id", "left", "right", "uid", "metadata", "name"),
        container="tuple",
    )
    assert list(returning) == [{"id": 0, "name": "root_new"}]
    assert list(row) == [(0, 1, 2, 100, None, "root_new")]


def test_update_all_rows():
    """Validate an update returning a dict."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = t.update("{left}={left}*{left}", literals={}, returning=("id", "left"), container="dict")
    row = t.select(
        "WHERE {id} = 0",
        columns=("id", "left", "right", "uid", "metadata", "name"),
        container="tuple",
    )
    assert list(returning) == [
        {"id": 0, "left": 1},
        {"id": 1, "left": 9},
        {"id": 2, "left": 25},
        {"id": 4, "left": 64},
        {"id": 5, "left": 100},
        {"id": 3, "left": 49},
        {"id": 7, "left": 169},
        {"id": 6, "left": None},
        {"id": 8, "left": None},
        {"id": 9, "left": None},
        {"id": 10, "left": None},
        {"id": 11, "left": None},
        {"id": 12, "left": None},
        {"id": 13, "left": None},
    ]
    assert list(row) == [(0, 1, 2, 100, None, "root")]


def test_delete():
    """Validate a delete returning a tuple."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = t.delete("{id}={target}", {"target": 7}, ("uid", "id"), container="tuple")
    row = t.select("WHERE {id} = 7", columns=("id", "left", "right", "uid", "metadata", "name"))
    assert list(returning) == [(107, 7)]
    assert list(row) == []


def test_delete_no_pk():
    """Validate a delete returning a dict without."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = list(t.delete("{id}={target}", {"target": 7}, ("uid",), container="pkdict"))
    row = t.select("WHERE {id} = 7", columns=("id", "left", "right", "uid", "metadata", "name"))
    assert len(returning) == 1
    assert list(row) == []


def test_discover_table():
    """Validate table discovery.

    Create a table rt1 and fill it with some data.
    Instanciate a table rt2 with no schema from the same DB & table name as rt1.
    rt1 and rt2 should point at the same table.
    """
    _logger.debug(stack()[0][3])
    config1 = deepcopy(_CONFIG)
    config1["data_files"] = []
    t1 = table(config1)
    values_dict = [
        {
            "id": 91,
            "left": 3,
            "right": 4,
            "uid": 901,
            "metadata": [1, 2],
            "name": "Harry",
        },
        {
            "id": 92,
            "left": 5,
            "right": 6,
            "uid": 902,
            "metadata": [],
            "name": "William",
        },
    ]
    t1.insert(values_dict)
    t2 = table({"database": _CONFIG["database"], "table": _CONFIG["table"]})
    data = t2.select(columns=values_dict[0].keys())
    assert list(data) == values_dict
    values_dict.append({"id": 0, "left": 1, "right": 2, "uid": 201, "metadata": [], "name": "Diana"})
    t2.insert([values_dict[-1]])
    data = t1.select(columns=values_dict[0].keys())
    assert list(data) == values_dict


def test_arbitrary_sql():
    """Execute some arbitrary SQL."""
    _logger.debug(stack()[0][3])
    config = deepcopy(_CONFIG)
    t = table(config)
    result = next(t.raw.arbitrary_sql("SELECT 2.0::REAL * 3.0::REAL"))[0]
    assert result == approx(6.0)
