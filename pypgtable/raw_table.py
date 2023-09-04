"""Simplified database table access."""

from copy import deepcopy
from json import load
from logging import DEBUG, Logger, NullHandler, getLogger
from os.path import join
from pprint import pformat
from time import sleep
from typing import Any, Iterable, Literal, Generator

from text_token import register_token_code, text_token
from psycopg2 import ProgrammingError, errors, sql

from .common import backoff_generator
from .database import db_connect, db_create, db_delete, db_exists, db_transaction
from .pypgtable_typing import RawCType
from .validators import raw_table_column_config_validator as rtccv
from .validators import raw_table_config_validator

_logger: Logger = getLogger(__name__)
_logger.addHandler(NullHandler())
_LOG_DEBUG: bool = _logger.isEnabledFor(DEBUG)


register_token_code("I05000", "SQL: {sql}")
register_token_code(
    "I05001",
    "Table {table} cannot be created as it already exists in database {dbname}.",
)
register_token_code(
    "I05003",
    "Table {table} in database {dbname} does not yet exist. Waiting {backoff:.2}s to retry.",
)
register_token_code("I05004", "Adding data to table {table} from {file}.")
register_token_code("I05005", "Database {dbname} does not yet exist. Waiting {backoff:.2}s to retry.")
register_token_code("E05000", "Configuration error: See lines below.\n{error}")
register_token_code(
    "E05001",
    "{set} columns differ between DB {dbname} and table {table} configuration.",
)
register_token_code(
    "E05002",
    "Existing database table {table} columns do not match configuration. Column {column} PRIMARY KEY constraint is inconsistent.",
)
register_token_code(
    "E05003",
    "Existing database table {table} columns do not match configuration. Column {column} UNIQUE constraint is inconsistent.",
)
register_token_code(
    "E05004",
    "Existing database table {table} columns do not match configuration. Column {column} NOT NULL constraint is inconsistent.",
)
register_token_code(
    "E05005",
    "Table {table} does not exist in database {dbname} and will not be created.",
)
register_token_code("E05006", "Recursive select on table {table} requires ptr_map to be configured.")


_INITIAL_DELAY = 0.125
_BACKOFF_STEPS = 13
_BACKOFF_FUZZ = True
_TYPE_ALIGNMENTS: dict[str, int] = {
    "BIGINT": 8,
    "BIGSERIAL": 8,
    "BOOL": 1,
    "BOOLEAN": 1,
    "CHAR": 1,
    "CHARACTER": 1,
    "DATE": 8,
    "DOUBLE PRECISION": 8,
    "INTEGER": 4,
    "INT4": 4,
    "INT": 4,
    "INTERVAL": 8,
    "REAL": 4,
    "FLOAT4": 4,
    "SMALLINT": 2,
    "INT2": 2,
    "SMALLSERIAL": 2,
    "SERIAL2": 2,
    "SERIAL": 4,
    "SERIAL4": 4,
    "TIME": 8,
    "TIMESTAMP": 8,
    "UUID": 8,
}
TYPES = tuple(_TYPE_ALIGNMENTS.keys())
_TABLE_LEN_SQL = sql.SQL("SELECT COUNT(*) FROM {0}")
_TABLE_EXISTS_SQL = sql.SQL("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {0})")
_TABLE_DEFINITION_SQL = sql.SQL(
    "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = {0}"
)
_TABLE_GET_PRIMARY_KEY_SQL = sql.SQL(
    (
        "SELECT c.column_name, tc.constraint_type FROM information_schema.table_constraints tc "
        "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) "
        "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema "
        "AND tc.table_name = c.table_name AND ccu.column_name = c.column_name "
        "WHERE tc.table_name = {0}"
    )
)
_TABLE_CREATE_SQL = sql.SQL("CREATE TABLE {0} ({1})")
_TABLE_INDEX_SQL = sql.SQL("CREATE INDEX {0} ON {1}")
_TABLE_INDEX_COLUMN_SQL = sql.SQL("({0})")
_TABLE_DELETE_TABLE_SQL = sql.SQL("DROP TABLE IF EXISTS {0} CASCADE")
_TABLE_RECURSIVE_SELECT = sql.SQL(
    "WITH RECURSIVE rq AS (SELECT {0} FROM {1} {2} UNION {5}SELECT {3} FROM {1} t INNER JOIN rq r ON {4}) SELECT * FROM rq"
)
_TABLE_SELECT_SQL = sql.SQL("SELECT {0} FROM {1} {2}")
_TABLE_INSERT_SQL = sql.SQL("INSERT INTO {0} ({1}) VALUES {2} ON CONFLICT ")
_TABLE_INSERT_CONFLICT_STR = "DO NOTHING"
_TABLE_UPSERT_CONFLICT_STR = "{0} DO UPDATE SET "
_TABLE_UPDATE_WHERE_SQL = sql.SQL("UPDATE {0} SET {1} WHERE {2}")
_TABLE_UPDATE_SQL = sql.SQL("UPDATE {0} SET {1}")
_TABLE_DELETE_SQL = sql.SQL("DELETE FROM {0} WHERE {1}")
_TABLE_RETURNING_SQL = sql.SQL(" RETURNING ")
_DEFAULT_UPDATE_STR = "{{{0}}}={{EXCLUDED.{0}}}"


def default_config():
    """Get a config template."""
    return raw_table_config_validator.sub_normalized({"table": "your_table_name"})


class raw_table:
    """Connects to (or creates as needed) a postgres database & table.

    The intention of raw_table is to provide a simple interface to instanciate,
    append, update & query a persistant data store using directly mapped database types.

    Whilst database_table acts like it has complete control over the defined databases
    it does not assume that it does. Once tables are created raw_table users
    need only have SELECT, INSERT & UPDATE privileges.
    """

    def __init__(self, config, populate=True) -> None:
        """Connect to or create all required objects.

        Args
        ----
        config (dict): The table configuration. See database/formats/raw_table_config_format.json.
        """
        self._primary_key = None
        self._entry_validator = None
        self.config = deepcopy(config)
        self._validate_config()
        self.creator = False
        self.db_creator = False
        self.populate = populate
        self._table = sql.Identifier(self.config["table"])
        self.ptr_map_def(self.config["ptr_map"])
        if self.config["delete_db"]:
            self.delete_db()
        if not db_exists(self.config["database"]["dbname"], self.config["database"]) and self.config["create_db"]:
            self._create_db()
        if self.config["delete_table"]:
            self.delete_table()
        create_table: bool = not self._table_exists() and self.config["create_table"]
        self.columns = self._create_table() if create_table else self._table_definition()
        if not create_table and not self.columns:
            raise ValueError(
                text_token(
                    {
                        "E05005": {
                            "table": self.config["table"],
                            "dbname": self.config["database"]["dbname"],
                        }
                    }
                )
            )

    def __len__(self) -> int:
        """Return the number of entries in the table."""
        return next(self._db_transaction(_TABLE_LEN_SQL.format(self._table)))[0]

    def _validate_config(self) -> None:
        """Validate the table configuration."""
        if not raw_table_config_validator.validate(self.config):
            raise ValueError(text_token({"E05000": {"error": raw_table_config_validator.error_str()}}))
        self.config = raw_table_config_validator.sub_normalized(self.config)

    def _get_primary_key(self) -> str | None:
        """Identify the primary key.

        Returns
        -------
        (str) column name of the primary key or None if there is no primary key.
        """
        if "schema" in self.config:
            for k, v in self.config["schema"].items():
                if v.get("primary_key", False):
                    return k
        return None

    def ptr_map_def(self, ptr_map: dict[str, str]) -> None:
        """Define how a recursive select traverses the graph.

        If the rows in the table define nodes in a graph then the pointer map defines
        the edges between nodes.

        self.config['ptr_map'] is of the form {
            "column X": "column Y",
            ...
        }
        where columns X contains a reference to a node identified by column Y.
        """
        pm_sql: list[sql.Composed] = [sql.SQL("r.") + sql.Identifier(r) + sql.SQL("=t.") + sql.Identifier(i) for r, i in ptr_map.items()]
        self._pm_sql: sql.Composed = sql.SQL(" OR ").join(pm_sql)
        self._pm: dict[str, str] = deepcopy(ptr_map)
        self._pm_columns: set[str] = set(ptr_map.keys()) | set(ptr_map.values())

    def _db_exists(self) -> bool:
        return db_exists(self.config["database"]["dbname"], self.config["database"])

    def _create_db(self) -> None:
        db_create(self.config["database"]["dbname"], self.config["database"])
        self.db_creator = True

    def delete_db(self) -> None:
        """Delete the database."""
        db_delete(self.config["database"]["dbname"], self.config["database"])

    def _db_transaction(self, sql_str, read=True, ctype="tuple"):
        """Wrap db_transaction."""
        if _LOG_DEBUG:
            _logger.debug(self._sql_to_string(sql_str))
        return db_transaction(
            self.config["database"]["dbname"],
            self.config["database"],
            sql_str,
            read,
            ctype=ctype,
        )

    def arbitrary_sql(
        self,
        sql_str: str,
        literals: dict[str, Any] | None = None,
        read: bool = True,
        ctype: RawCType = "tuple",
    ):
        """Exectue the arbitrary SQL string sql_str.

        The string is passed to psycopg2 to execute.
        Column names and literals will be formatted (see select() as an example)
        On your head be it.

        Args
        ----
        sql_str (str): SQL string to be executed.
        literals (dict): Keys are labels used in sql_str. Values are literals to replace the labels.
        read (bool): True if the SQL does not make changes to the database.
        ctype (str): One of 'tuple', 'namedtuple', 'dict'

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype:
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        format_dict: dict[str, sql.Identifier | sql.Literal] = self._format_dict(literals)
        _sql_str: sql.Composed = sql.SQL(sql_str).format(**format_dict)
        return self._db_transaction(_sql_str, read, ctype)

    def _sql_to_string(self, sql_str) -> str:
        """Wrap sql.SQL.as_string() to convert sql.SQL to a string (usually for logging)."""
        return sql_str.as_string(db_connect(self.config["database"]["dbname"], self.config["database"]))

    def _populate_table(self) -> None:
        """Add data to table after creation.

        Data is inserted into the table in batches of consecutive rows
        that have the same keys defined.
        This preserves order and allows columns to be set to NULL or
        their DEFAULT values.

        Only executed if this instance of raw_table() created it.
        See self._create_table().
        """
        if self.populate and self.config["data_files"]:
            for data_file in self.config["data_files"]:
                abspath: str = join(self.config["data_file_folder"], data_file)
                _logger.info(text_token({"I05004": {"table": self.config["table"], "file": abspath}}))
                with open(abspath, "r", encoding="utf-8") as file_ptr:
                    for columns, values in self.batch_dict_data(load(file_ptr)):
                        self.insert(columns, values)

    def batch_dict_data(self, data, exclude=tuple(), ordered=False):
        """Generate to break up an iterable of dictionaries into batches with the same keys.

        The order of dictionaries in the iterable is not preserved by default.
        Data keys that are not table columns are filtered out.

        Args
        ----
        data (iter(dict)): Each dict is a subset of a table row.
        exclude (iter(str)): Iterable of columns to exclude.
        ordered (bool): Maintain row order (this may matter in some corner cases)

        Returns
        -------
        tuple(keys), (list(list)): A consectutive batch of rows with the same keys.
        """
        set_of_columns: set[str] = set(self.columns) - set(exclude)
        if ordered:
            okeys: tuple[str, ...] = tuple()
            last_datum_keys = set()
            current_batch: list[list[Any]] = []
            for datum in data:
                datum_keys: set[str] = set(datum.keys()) & set_of_columns
                if last_datum_keys == set(datum_keys):
                    current_batch.append([datum[k] for k in okeys])
                else:
                    if current_batch:
                        yield okeys, current_batch
                    okeys = tuple(datum_keys)
                    current_batch = [[datum[k] for k in okeys]]
                    last_datum_keys = set(datum_keys)
            yield okeys, current_batch
        else:
            batches: dict[str, list[Any]] = {}
            ordered_keys: dict[str, tuple[str, ...]] = {}
            for datum in data:
                datum_keys = set(datum.keys()) & set_of_columns
                datum_keys_hash: str = "".join(sorted(datum_keys))
                batches.setdefault(datum_keys_hash, [])
                ordered_keys.setdefault(datum_keys_hash, tuple(datum_keys))
                batches[datum_keys_hash].append([datum[k] for k in ordered_keys[datum_keys_hash]])
            for datum_keys_hash, batch in batches.items():
                yield ordered_keys[datum_keys_hash], batch

    def _table_definition(self) -> set[str]:
        """Get the table schema when it is defined in the database.

        Validate that the DB table has the same columns as the configuration schema if it is defined.

        Returns
        -------
        (tuple(str)): Column names.
        """
        backoff_gen: Generator[Any, None, None] = backoff_generator(_INITIAL_DELAY, _BACKOFF_STEPS, _BACKOFF_FUZZ)
        while not self._table_exists() and self.config["wait_for_table"]:
            backoff = next(backoff_gen)
            dbname = self.config["database"]["dbname"]
            if _LOG_DEBUG:
                _logger.debug(
                    text_token(
                        {
                            "I05003": {
                                "table": self.config["table"],
                                "dbname": dbname,
                                "backoff": backoff,
                            }
                        }
                    )
                )
            sleep(backoff)
        results = tuple(self._db_transaction(_TABLE_DEFINITION_SQL.format(sql.Literal(self.config["table"]))))
        columns = set((column[0] for column in results))
        schema = {c: rtccv.normalized({"type": d.upper(), "nullable": n == "YES"}) for c, d, n in results}
        constraints = self._db_transaction(_TABLE_GET_PRIMARY_KEY_SQL.format(sql.Literal(self.config["table"])))
        for column, constraint in constraints:
            schema[column]["primary_key"] = (constraint == "PRIMARY KEY") or schema[column].get("primary_key", False)
            schema[column]["unique"] = (constraint == "UNIQUE") or schema[column].get("primary_key", False)

        self.config.setdefault("schema", schema)
        self._primary_key = self._get_primary_key()
        _logger.debug(f"Table {self.config['table']} schema:\n{pformat(self.config['schema'])}")

        unmatched_set = columns - set(self.config["schema"].keys())
        if unmatched_set:
            token = text_token(
                {
                    "E05001": {
                        "set": unmatched_set,
                        "dbname": self.config["database"]["dbname"],
                        "table": self.config["table"],
                    }
                }
            )
            _logger.error(token)
            raise ValueError(token)

        for column in columns:
            if schema[column]["primary_key"] != self.config["schema"][column]["primary_key"]:
                raise ValueError(text_token({"E05002": {"table": self.config["table"], "column": column}}))
            if schema[column]["unique"] != self.config["schema"][column]["unique"]:
                raise ValueError(text_token({"E05003": {"table": self.config["table"], "column": column}}))
            if schema[column]["nullable"] != self.config["schema"][column]["nullable"]:
                raise ValueError(text_token({"E05004": {"table": self.config["table"], "column": column}}))
        return columns

    def _table_exists(self):
        """Test if the table exists in the database.

        Returns
        -------
        (bool) True if the table exists else False.
        """
        backoff_gen = backoff_generator(_INITIAL_DELAY, _BACKOFF_STEPS, _BACKOFF_FUZZ)
        while not db_exists(self.config["database"]["dbname"], self.config["database"]) and self.config["wait_for_db"]:
            backoff = next(backoff_gen)
            _logger.info(
                text_token(
                    {
                        "I05005": {
                            "dbname": self.config["database"]["dbname"],
                            "backoff": backoff,
                        }
                    }
                )
            )
            sleep(backoff)
        return next(self._db_transaction(_TABLE_EXISTS_SQL.format(sql.Literal(self.config["table"]))))[0]

    def _add_alignment(self, definition):
        """Add the byte alignment of the column type to the column definition.

        Alignment depends on the column type and is an integer number of bytes usually
        1, 2, 4 or 8. A value of 0 is used to define a variable alignment field.
        Args
        ----
        definition (dict): Column definition as defined by raw_table_column_config_format.json

        Returns
        -------
        (dict): A column definition plus an 'alignment' field.
        """
        upper_type = definition["type"].upper()
        array_idx = upper_type.find("[")
        fixed_length = upper_type.find("[]") == -1
        if array_idx != -1:
            upper_type = upper_type[:array_idx]
        definition["alignment"] = _TYPE_ALIGNMENTS.get(upper_type.strip(), 0) if fixed_length else 0
        return definition

    def _order_schema(self):
        """Order table columns to minimise disk footprint.

        A small performance/resource benefit can be gleaned from ordering the columns
        of a table to reduce packing/alignment costs.
        See https://stackoverflow.com/questions/12604744/does-the-order-of-columns-in-a-postgres-table-impact-performance

        Returns
        -------
        (list(tuple(str, dict))): Tuples are (column name, definition) sorted in descending alignment
        requirment i.e. largest to smallest, with variable l
        """
        definition_list = [(c, self._add_alignment(d)) for c, d in self.config["schema"].items()]
        return sorted(definition_list, key=lambda x: str(x[1]["alignment"]) + x[0], reverse=True)

    def _create_table(self):
        """Create the table if it does not exists and the user has privileges to do so.

        Assumption is that other processes may also be trying to create the table and so
        duplicate table (or privilege) exceptions are not considered errors just a race condition
        to wait out. If this process does create the table then it will set the self.creator flag.

        Returns
        -------
        (tuple(str)) Column names.
        """
        columns: list[str | sql.Composed] = []
        self.columns: set[str] = set()
        definition_list = self._order_schema()
        _logger.info("Table will be created with columns in the order logged below.")
        for column, definition in definition_list:
            sql_str = " " + definition["type"]
            if not definition["nullable"]:
                sql_str += " NOT NULL"
            if definition["primary_key"]:
                sql_str += " PRIMARY KEY"
            if definition["unique"] and not definition["primary_key"]:
                sql_str += " UNIQUE"
            if "default" in definition:
                sql_str += " DEFAULT " + definition["default"]
            self.columns.add(column)
            _logger.info(f"Column: {column}, SQL Definition: {sql_str}, Alignment: {definition['alignment']}")
            columns.append(sql.Identifier(column) + sql.SQL(sql_str))

        sql_str = _TABLE_CREATE_SQL.format(self._table, sql.SQL(", ").join(columns))
        _logger.info(text_token({"I05000": {"sql": self._sql_to_string(sql_str)}}))
        try:
            self._db_transaction(sql_str, read=False)
        except ProgrammingError as exc:
            if exc.pgcode == errors.DuplicateTable:  # pylint: disable=no-member
                _logger.info(
                    text_token(
                        {
                            "I05001": {
                                "table": self.config["table"],
                                "dbname": self.config["database"],
                            }
                        }
                    )
                )
                return self._table_definition()
            raise exc

        self._create_indices()
        self.creator = True
        self._populate_table()
        return self._table_definition()

    def _create_indices(self) -> None:
        """Create an index for columns that specify one."""
        for column, definition in filter(lambda x: "index" in x[1], self.config["schema"].items()):
            sql_str = _TABLE_INDEX_SQL.format(
                sql.Identifier(self.config["table"] + "_" + column + "_index"),
                self._table,
            )
            sql_str += sql.SQL(" USING ") + sql.Identifier(definition["index"])
            sql_str += _TABLE_INDEX_COLUMN_SQL.format(sql.Identifier(column))
            _logger.info(text_token({"I05000": {"sql": self._sql_to_string(sql_str)}}))
            self._db_transaction(sql_str, read=False)

    def delete_table(self) -> None:
        """Delete the table."""
        if db_exists(self.config["database"]["dbname"], self.config["database"]):
            sql_str: sql.Composed = _TABLE_DELETE_TABLE_SQL.format(self._table)
            _logger.info(text_token({"I05000": {"sql": self._sql_to_string(sql_str)}}))
            self._db_transaction(sql_str, read=False)

    def select(
        self,
        query_str: str = "",
        literals: dict[str, Any] | None = None,
        columns: Literal["*"] | Iterable[str] = "*",
        ctype: RawCType = "tuple",
    ):
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
        ctype (str): One of 'tuple', 'namedtuple', 'dict'

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype:
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        if columns == "*":
            columns = self.columns
        format_dict: dict[str, sql.Identifier | sql.Literal] = self._format_dict(literals)
        if isinstance(columns, str):
            _columns: sql.Composed = sql.SQL(columns).format(**format_dict)
        else:
            _columns = sql.SQL(", ").join(map(sql.Identifier, columns))
        sql_str: sql.Composed = _TABLE_SELECT_SQL.format(_columns, self._table, sql.SQL(query_str).format(**format_dict))
        return self._db_transaction(sql_str, ctype=ctype)

    # TODO: Add delta (results in A but not in B) & intersection (results in A & B) recursive queries
    # https://www.postgresql.org/docs/8.3/queries-union.html

    def recursive_select(
        self,
        query_str: str,
        literals: dict[str, Any] | None = None,
        columns: Literal["*"] | Iterable[str] = "*",
        ctype: RawCType = "tuple",
        dedupe: bool = True,
    ):
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
        ctype (str): One of 'tuple', 'namedtuple', 'dict'
        dedupe (bool): Duplicate entries are removed from the result when True.

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype of the values specified by columns for the specified
        recursive query_str and pointer map.
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        if not self._pm:
            raise ValueError(text_token({"E05006": {"table": self.config["table"]}}))

        if columns == "*":
            columns = self.columns
        else:
            columns = list(columns)
            for ptr in self._pm_columns:
                if ptr not in columns:
                    columns.append(ptr)
        t_columns: sql.Composed = sql.SQL("t.") + sql.SQL(", t.").join(map(sql.Identifier, columns))
        _columns: sql.Composed = sql.SQL(", ").join(map(sql.Identifier, columns))
        format_dict: dict[str, sql.Identifier | sql.Literal] = self._format_dict(literals)
        sql_str: sql.Composed = _TABLE_RECURSIVE_SELECT.format(
            _columns,
            self._table,
            sql.SQL(query_str).format(**format_dict),
            t_columns,
            self._pm_sql,
            sql.SQL(("ALL ", "")[dedupe]),
        )
        return self._db_transaction(sql_str, ctype=ctype)

    def _format_dict(self, literals: dict[str, Any]) -> dict[str, sql.Identifier | sql.Literal]:
        """Create a formatting dict of literals and column identifiers."""
        dupes: set[str] = set(literals.keys()).intersection(self.columns)
        if dupes:
            raise ValueError(f"Literals cannot have keys that are the names of table columns:{dupes}")
        format_dict: dict[str, sql.Identifier | sql.Literal] = {k: sql.Identifier(k) for k in self.columns}
        format_dict.update({k: sql.Literal(v) for k, v in literals.items()})
        return format_dict

    # TODO: This could overflow an SQL statement size limit. In which case
    # should we use a COPY https://www.postgresql.org/docs/12/dml-insert.html
    def upsert(
        self,
        columns,
        values,
        update_str=None,
        literals: dict[str, Any] | None = None,
        returning=tuple(),
        ctype: RawCType = "tuple",
    ):
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
        ctype (str): One of 'tuple', 'namedtuple', 'dict'

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype of the values specified by returning for each updated row:
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        if returning == "*":
            returning = self.columns
        if update_str is None:
            update_str = ",".join((_DEFAULT_UPDATE_STR.format(k) for k in columns if k != self._primary_key))
        if update_str != _TABLE_INSERT_CONFLICT_STR:
            if self._primary_key is None:
                raise ValueError("Can only upsert if a primary key is defined.")
            update_str = _TABLE_UPSERT_CONFLICT_STR.format("({" + self._primary_key + "})") + update_str
        columns_sql = sql.SQL(",").join([sql.Identifier(k) for k in columns])
        values_sql = sql.SQL(",").join(
            (sql.SQL("({0})").format(sql.SQL(",").join((sql.Literal(value) for value in row))) for row in values)
        )
        if not values_sql.seq:
            return iter(tuple())
        format_dict = self._format_dict(literals)
        format_dict.update({"EXCLUDED." + k: sql.SQL("EXCLUDED.") + sql.Identifier(k) for k in columns})
        update_sql = sql.SQL(update_str).format(**format_dict)
        if returning:
            update_sql += _TABLE_RETURNING_SQL + sql.SQL(",").join([sql.Identifier(column) for column in returning])
        return self._db_transaction(
            _TABLE_INSERT_SQL.format(self._table, columns_sql, values_sql) + update_sql,
            read=False,
            ctype=ctype,
        )

    def insert(self, columns, values, returning=tuple()):
        """Insert values.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (iter(tuple/list)): Iterable of rows (ordered iterables) with values in the order as columns.
        returning (iter): The columns to be returned on update. If None or empty no columns will be returned.
        """
        return self.upsert(columns, values, _TABLE_INSERT_CONFLICT_STR, returning=returning)

    def update(
        self,
        update_str,
        query_str=None,
        literals: dict[str, Any] | None = None,
        returning=tuple(),
        ctype="tuple",
    ):
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
        ctype (str): One of 'tuple', 'namedtuple', 'dict'

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype of the values specified by returning for each updated row:
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        if returning == "*":
            returning = self.columns
        format_dict = self._format_dict(literals)
        if query_str is not None:
            sql_str = _TABLE_UPDATE_WHERE_SQL.format(
                self._table,
                sql.SQL(update_str).format(**format_dict),
                sql.SQL(query_str).format(**format_dict),
            )
        else:
            sql_str = _TABLE_UPDATE_SQL.format(self._table, sql.SQL(update_str).format(**format_dict))
        if returning:
            sql_str += _TABLE_RETURNING_SQL + sql.SQL(",").join([sql.Identifier(column) for column in returning])
        return self._db_transaction(sql_str, read=False, ctype=ctype)

    def delete(
        self,
        query_str,
        literals: dict[str, Any] | None = None,
        returning=tuple(),
        ctype: RawCType = "tuple",
    ):
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
        ctype (str): One of 'tuple', 'namedtuple', 'dict'

        Returns
        -------
        A psycopg2 cursor of a type defined by ctype of the values specified by returning for each updated row:
            'tuple': TupleCursor
            'namedtuple': NamedTupleCursor
            'dict': DictCursor
        """
        if literals is None:
            literals = {}
        if returning == "*":
            returning = self.columns
        format_dict = self._format_dict(literals)
        sql_str = _TABLE_DELETE_SQL.format(self._table, sql.SQL(query_str).format(**format_dict))
        if returning:
            sql_str += _TABLE_RETURNING_SQL + sql.SQL(",").join([sql.Identifier(column) for column in returning])
        return self._db_transaction(sql_str, read=False, ctype=ctype)
