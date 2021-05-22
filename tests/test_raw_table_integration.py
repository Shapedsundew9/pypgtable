"""Unit tests for raw_table.py."""


from copy import deepcopy
from os.path import join, dirname
from pypgtable.raw_table import raw_table
from pypgtable.utils.base_logging import get_logger


_logger, _ = get_logger(__file__, __name__)


_CONFIG = {
    'database': {
        'dbname': 'test_db',
        'host': 'postgres'
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


def test_create_table():
    """Validate a the SQL sequence when a table exists."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    assert not rt is None


def test_select():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    data = rt.select('WHERE {id} = {seven}', {'seven': 7}, columns=('uid', 'left', 'right'))
    assert data == [(107, 13, None)]


def test_recursive_select():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    data = rt.recursive_select('WHERE {id} = 2', columns=('id', 'uid', 'left', 'right'))
    assert data == [(2, 102, 5, 6), (5, 105, 10, 11), (6, 106, None, 12),
        (10, 110, None, None), (11, 111, None, None), (12, 112, None, None)]


def test_insert():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (92, 5, 6, 902, [], "William"))
    rt.insert(columns, values)
    data = tuple(rt.select('WHERE {id} > 90', columns=columns))
    assert data == values


def test_upsert():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    columns = ("id", "left", "right", "uid", "metadata", "name")
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (0, 1, 2, 201, [], "Diana"))
    returning = rt.upsert(columns, values, '{name}={EXCLUDED.name} || {temp}', {'temp': '_temp'}, ('uid', 'id', 'name'))
    row = rt.select('WHERE {id} = 0', columns=('id', 'left', 'right', 'uid', 'metadata', 'name'))
    assert returning == [(901, 91, 'Harry'), (100, 0, 'Diana_temp')]
    assert  row == [(0, 1, 2, 100, None, "Diana_temp")]


def test_update():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    returning = rt.update('{name}={name} || {new}', '{id}={qid}', {'qid':0, 'new': '_new'}, ('id', 'name'))
    row = rt.select('WHERE {id} = 0', columns=('id', 'left', 'right', 'uid', 'metadata', 'name'))
    assert returning == [(0, 'root_new')]
    assert row == [(0, 1, 2, 100, None, "root_new")]


def test_delete():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    returning = rt.delete('{id}={target}', {'target': 7}, ('uid', 'id'))
    row = rt.select('WHERE {id} = 7', columns=('id', 'left', 'right', 'uid', 'metadata', 'name'))
    assert returning == [(107, 7)]
    assert row == []


def test_validate():
    """As it says on the tin."""
    config = deepcopy(_CONFIG)
    rt = raw_table(config)
    columns = ('id', 'left', 'right', 'uid', 'metadata', 'name')
    values = ((91, 3, 4, 901, [1, 2], "Harry"), (92, 5, 6, 902, [], "William"))
    results = rt.validate(columns, values)
    assert len(results) == len(values)
    assert all(results)


def test_duplicate_table():
    """Validate a the SQL sequence when a table exists."""
    config1 = deepcopy(_CONFIG)
    config2 = deepcopy(_CONFIG)
    config2['delete_table'] = False
    rt1 = raw_table(config1)
    rt2 = raw_table(config2)
    for t1, t2 in zip(rt1.select(columns=('updated',))[0], rt2.select(columns=('updated',))[0]):
        assert t1 == t2
    rt1.delete_table()
    rt2.delete_table()
