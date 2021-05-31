"""Unit tests for raw_table.py."""

from copy import deepcopy
from os.path import join, dirname
from pypgtable.table import table
from pypgtable.utils.base_logging import get_logger


_logger, _ = get_logger(__file__, __name__)


_CONFIG = {
    'database': {
        'dbname': 'test_db'
    },
    'table': 'test_table',
    'schema': {
        'name': {
            'type': 'VARCHAR',
            'nullable': True
        },
        'id': {
            'type': 'INTEGER',
            'primary_key': True
        },
        'left': {
            'type': 'INTEGER',
            'nullable': True
        },
        'right': {
            'type': 'INTEGER',
            'nullable': True
        },
        'uid': {
            'type': 'INTEGER',
            'index': 'btree',
            'unique': True,
        },
        'updated': {
            'type': 'TIMESTAMP',
            'default': 'NOW()'
        },
        'metadata': {
            'type': 'INTEGER[]',
            'index': 'btree',
            'nullable': True
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


def _register_conversions(table):
    table.register_conversion('id', lambda x: x - 1000, lambda x: x + 1000)
    table.register_conversion('name', lambda x: x.lower(), lambda x: x.upper())
    return table


def test_create_table():
    """Validate a the SQL sequence when a table exists."""
    config = deepcopy(_CONFIG)
    t = table(config)
    assert t is not None


def test_getitem_encoded_pk1():
    """Validate a valid getitem for an encoded primary key."""
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    expected = {"id": 1000, "left": 1, "right": 2,
                "uid": 100, "metadata": None, "name": "ROOT"}
    result = t[1000]
    assert all([expected[k] == result[k] for k in expected])


def test_getitem_encoded_pk2():
    """Validate an invalid getitem for an encoded primary key."""
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    assert t[0] == {}


def test_getitem_pk1():
    """Validate a valid getitem."""
    config = deepcopy(_CONFIG)
    t = table(config)
    expected = {"id": 0, "left": 1, "right": 2,
                "uid": 100, "metadata": None, "name": "root"}
    result = t[0]
    assert all([expected[k] == result[k] for k in expected])


def test_getitem_pk2():
    """Validate an invalid getitem."""
    config = deepcopy(_CONFIG)
    t = table(config)
    assert t[1000] == {}


def test_getitem_no_pk():
    """Validate if the table has no primary key we get the correct ValueError."""
    config = deepcopy(_CONFIG)
    config['schema']['id']['primary_key'] = False
    t = table(config)
    try:
        t[0]
    except ValueError as e:
        assert str(e) == "SELECT row on primary key but no primary key defined!"
    else:
        assert False


def test_setitem_encoded_pk():
    """Validate a valid setitem for an encoded primary key."""
    config = deepcopy(_CONFIG)
    t = _register_conversions(table(config))
    setitem = {"id": 22, "left": 9, "right": 12,
               "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    expected_decoded = {"id": 22, "left": 9, "right": 12,
                        "uid": 122, "metadata": [34, 78], "name": "ROOT"}
    expected_raw = {"id": -978, "left": 9, "right": 12,
                    "uid": 122, "metadata": [34, 78], "name": "root"}
    t[22] = setitem
    result = t[22]
    raw_result = dict(
        zip(t.raw._columns, t.raw.select('WHERE {id} = -978')[0]))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])


def test_setitem_pk():
    """Validate a valid setitem."""
    config = deepcopy(_CONFIG)
    t = table(config)
    setitem = {"id": 22, "left": 9, "right": 12,
               "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    expected_decoded = {"id": 22, "left": 9, "right": 12,
                        "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    expected_raw = {"id": 22, "left": 9, "right": 12,
                    "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    t[22] = setitem
    result = t[22]
    raw_result = dict(zip(t.raw._columns, t.raw.select('WHERE {id} = 22')[0]))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])


def test_setitem_mismatch_pk():
    """When setting an item and specifying the primary key in the value the setitem key takes precedence."""
    config = deepcopy(_CONFIG)
    t = table(config)
    setitem = {"id": 22, "left": 9, "right": 12,
               "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    expected_decoded = {"id": 28, "left": 9, "right": 12,
                        "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    expected_raw = {"id": 28, "left": 9, "right": 12,
                    "uid": 122, "metadata": [34, 78], "name": "rOoT"}
    t[28] = setitem
    result = t[28]
    raw_result = dict(zip(t.raw._columns, t.raw.select('WHERE {id} = 28')[0]))
    assert all([expected_decoded[k] == result[k] for k in expected_decoded])
    assert all([expected_raw[k] == raw_result[k] for k in expected_raw])
    assert t[22] == {}


def test_select_dict():
    """Validate select returning a dict."""
    config = deepcopy(_CONFIG)
    t = table(config)
    data = t.select('WHERE {id} = {seven}', {'seven': 7},
                    columns=('uid', 'left', 'right'))
    assert data == {7: {'id': 7, 'uid': 107, 'left': 13, 'right': None}}


def test_select_list():
    """Validate select returning a list."""
    config = deepcopy(_CONFIG)
    t = table(config)
    data = t.select('WHERE {id} = {seven}', {'seven': 7}, columns=(
        'uid', 'left', 'right'), container='list')
    assert data == [[107, 13, None]]


def test_select_tuple():
    """Validate select returning a list."""
    config = deepcopy(_CONFIG)
    t = table(config)
    data = t.select('WHERE {id} = {seven}', {'seven': 7}, columns=(
        'uid', 'left', 'right'), container='tuple')
    assert data == [(107, 13, None)]


def test_recursive_select():
    """Validate a recursive select returning a tuple."""
    config = deepcopy(_CONFIG)
    t = table(config)
    data = t.recursive_select('WHERE {id} = 2', columns=(
        'id', 'uid', 'left', 'right'), container='tuple')
    assert data == [(2, 102, 5, 6), (5, 105, 10, 11), (6, 106, None, 12),
                    (10, 110, None, None), (11, 111, None, None), (12, 112, None, None)]


def test_upsert():
    """Validate an upsert consisting or 1 insert & 1 update returing updated fields as tuples."""
    _logger.debug('')
    config = deepcopy(_CONFIG)
    t = table(config)
    data = (
        {'id': 91, 'left': 3, 'right': 4, 'uid': 901,
            'metadata': [1, 2], 'name': 'Harry'},
        {'id': 0, 'left': 1, 'right': 2, 'uid': 201, 'metadata': [], 'name': 'Diana'}
    )
    returning = t.upsert(data, '{name}={EXCLUDED.name} || {temp}', {
                         'temp': '_temp'}, ('uid', 'id', 'name'), container='tuple')
    row = t.select('WHERE {id} = 0', columns=(
        'id', 'left', 'right', 'uid', 'metadata', 'name'), container='tuple')
    assert returning == [(901, 91, 'Harry'), (100, 0, 'Diana_temp')]
    assert row == [(0, 1, 2, 100, None, "Diana_temp")]


def test_insert():
    """Validate inserting two rows from a dict."""
    config = deepcopy(_CONFIG)
    t = table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    data = [
        {'id': 91, 'left': 3, 'right': 4, 'uid': 901,
            'metadata': [1, 2], 'name': 'Harry'},
        {'id': 92, 'left': 5, 'right': 6, 'uid': 902,
            'metadata': [], 'name': 'William'}
    ]
    t.insert(data)
    results = list(t.select('WHERE {id} > 90 ORDER BY {id} ASC', columns=columns).values())
    assert data == results


def test_update():
    """Validate an update returning a dict."""
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = t.update('{name}={name} || {new}', '{id}={qid}', {
                         'qid': 0, 'new': '_new'}, ('id', 'name'))
    row = t.select('WHERE {id} = 0', columns=(
        'id', 'left', 'right', 'uid', 'metadata', 'name'), container='tuple')
    assert returning == {0: {'id': 0, 'name': 'root_new'}}
    assert row == [(0, 1, 2, 100, None, "root_new")]


def test_delete():
    """Validate a delete returning a list."""
    config = deepcopy(_CONFIG)
    t = table(config)
    returning = t.delete('{id}={target}', {'target': 7},
                         ('uid', 'id'), container='list')
    row = t.select('WHERE {id} = 7', columns=(
        'id', 'left', 'right', 'uid', 'metadata', 'name'))
    assert returning == [[107, 7]]
    assert row == {}
