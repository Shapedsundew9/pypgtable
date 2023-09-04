"""pypgtable typing."""
from typing import Any, Callable, LiteralString, NotRequired, TypedDict, Literal

from .row_iterators import dict_iter, gen_iter, namedtuple_iter, tuple_iter


class DatabaseConfig(TypedDict):
    """Database configuration."""

    dbname: NotRequired[str]
    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str | None]
    port: NotRequired[int]
    maintenance_db: NotRequired[str]
    retries: NotRequired[int]


class DatabaseConfigNorm(TypedDict):
    """Normalized database configuration."""

    dbname: str
    host: str
    user: str
    password: str | None
    port: int
    maintenance_db: str
    retries: int


class SchemaColumn(TypedDict):
    """Table schema column definition."""

    type: str
    volatile: NotRequired[bool]
    default: NotRequired[str]
    description: NotRequired[str]
    nullable: NotRequired[bool]
    primary_key: NotRequired[bool]
    index: NotRequired[str]
    unique: NotRequired[bool]


class SchemaColumnNorm(TypedDict):
    """Normalized table schema column definition."""

    type: str
    volatile: bool
    default: NotRequired[str]
    description: str
    nullable: bool
    primary_key: bool
    index: NotRequired[str]
    unique: bool


ConversionFunc = Callable[[Any], Any] | None
Conversion = tuple[LiteralString, ConversionFunc, ConversionFunc] | list[LiteralString | ConversionFunc]
Conversions = tuple[Conversion, ...]
PtrMap = dict[str, str]
TableSchema = dict[str, SchemaColumn]


class TableConfig(TypedDict):
    """Table configuration."""

    database: NotRequired[DatabaseConfig]
    table: str
    schema: TableSchema
    ptr_map: NotRequired[PtrMap]
    data_file_folder: NotRequired[str]
    data_files: NotRequired[list[str]]
    delete_db: NotRequired[bool]
    delete_table: NotRequired[bool]
    create_db: NotRequired[bool]
    create_table: NotRequired[bool]
    wait_for_db: NotRequired[bool]
    wait_for_table: NotRequired[bool]
    conversions: NotRequired[Conversions]


TableSchemaNorm = dict[str, SchemaColumnNorm]


class TableConfigNorm(TypedDict):
    """Normalized table configuration."""

    database: DatabaseConfigNorm
    table: str
    schema: TableSchemaNorm
    ptr_map: PtrMap
    data_file_folder: str
    data_files: list[str]
    delete_db: bool
    delete_table: bool
    create_db: bool
    create_table: bool
    wait_for_db: bool
    wait_for_table: bool
    conversions: Conversions


RowIter = tuple_iter | namedtuple_iter | gen_iter | dict_iter
RawCType = Literal["tuple", "namedtuple", "dict"]
