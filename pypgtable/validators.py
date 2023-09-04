"""Validators for pypgtable."""
from copy import deepcopy
from json import load
from logging import Logger, NullHandler, getLogger
from os.path import dirname, join
from typing import Any

from .base_validator import base_validator

_logger: Logger = getLogger(__name__)
_logger.addHandler(NullHandler())


with open(join(dirname(__file__), "formats/database_config_format.json"), "r", encoding="utf8") as file_ptr:
    PYPGTABLE_DB_CONFIG_SCHEMA: dict[str, dict[str, Any]] = load(file_ptr)
database_config_validator: base_validator = base_validator(PYPGTABLE_DB_CONFIG_SCHEMA, purge_unknown=True)
with open(
    join(dirname(__file__), "formats/raw_table_column_config_format.json"),
    "r",
    encoding="utf8",
) as file_ptr:
    PYPGTABLE_COLUMN_CONFIG_SCHEMA: dict[str, dict[str, Any]] = load(file_ptr)
raw_table_column_config_validator: base_validator = base_validator(PYPGTABLE_COLUMN_CONFIG_SCHEMA, purge_unknown=True)


class _raw_table_config_validator(base_validator):
    def sub_normalized(self, document):
        """Normalize sub-documents."""
        document = deepcopy(document)
        document["database"] = database_config_validator.normalized(document.get("database", {}))
        if "schema" in document:
            for column, definition in document["schema"].items():
                definition["unique"] = definition.get("unique", False) or definition.get("primary_key", False)
                document["schema"][column] = raw_table_column_config_validator.normalized(definition)
        return self.normalized(document)

    def _check_with_valid_database_config(self, field: str, value: Any) -> None:
        """Validate database configuration."""
        if not database_config_validator.validate(value):
            _logger.debug(f"Database config validator errors:\n{database_config_validator.error_str()}")
            self._error(field, database_config_validator.error_str())

    def _check_with_valid_raw_table_column_config(self, field: str, value: Any) -> None:
        """Validate every column configuration."""
        if not raw_table_column_config_validator.validate(value):
            _logger.debug(f"Raw table column {field} config validator errors:\n{raw_table_column_config_validator.error_str()}")
            self._error(field, raw_table_column_config_validator.error_str())
        if value.get("nullable", False) and value.get("primary_key", False):
            self._error(field, "A column cannot be both NULL and the PRIMARY KEY.")

    def _check_with_valid_schema_config(self, field: str, value: Any) -> None:
        """Validate the overall schema. There can be only one primary key."""
        primary_key_count = sum((config.get("primary_key", False) for config in value.values()))
        if primary_key_count > 1:
            self._error(
                field,
                f"There are {primary_key_count} primary keys defined. There can only be 0 or 1.",
            )

    def _check_with_valid_ptr_map_config(self, field: str, value: Any) -> None:
        """Validate pointer map configuration."""
        for k, v in value.items():
            if v in value.keys():
                self._error(field, f"Circular reference {v} -> {value[v]}")
            if "schema" in self.document:
                if k not in self.document["schema"].keys():
                    self._error(field, f"Key {k} is not a field.")
                if v not in self.document["schema"].keys():
                    self._error(field, f"Value {v} is not a field.")
            else:
                _logger.info("Table schema will be auto-discovered. Cannot validate ptr_map columns exist.")

    def _check_with_valid_file_folder(self, field: str, value: Any) -> None:
        """Validate data file folder exist if validate is set."""
        self._isdir(field, value)

    def _check_with_valid_data_files(self, field: str, value: Any) -> None:
        """Validate the data files if validate is set."""
        for filename in value:
            abspath: str = join(self.document["data_file_folder"], filename)
            if self._isjsonfile(field, abspath) is None:
                self._error(field, f"Data file {abspath} is invalid.")

    def _check_with_valid_delete_db(self, field: str, value: Any) -> None:
        """Validate delete_db."""
        if value and (not self.document.get("create_db", False) or self.document.get("wait_for_db", False)):
            self._error(
                field,
                "delete_db == True requires create_db == True and wait_for_db == False",
            )
        if value and not (self.document.get("create_table", False) or self.document.get("wait_for_table", False)):
            self._error(
                field,
                "delete_db == True requires either create_table == True or wait_for_table == True",
            )

    def _check_with_valid_delete_table(self, field: str, value: Any) -> None:
        """Validate delete_table."""
        if value and (not self.document.get("create_table", False) or self.document.get("wait_for_table", False)):
            self._error(
                field,
                "delete_table == True requires create_table == True and wait_for_table == False",
            )

    def _check_with_valid_create_db(self, field: str, value: Any) -> None:
        """Validate create_db."""
        if value and self.document.get("wait_for_db", False):
            self._error(field, "create_db == True requires wait_for_db == False")
        if value and not (self.document.get("create_table", False) or self.document.get("wait_for_table", False)):
            self._error(
                field,
                "create_db == True requires either create_table == True or wait_for_table == True",
            )

    def _check_with_valid_create_table(self, field: str, value: Any) -> None:
        """Validate create_table."""
        if value and self.document.get("wait_for_table", False):
            self._error(field, "create_table == True requires wait_for_table == False")

    def _check_with_valid_wait_for_db(self, field: str, value: Any) -> None:
        """Validate wait_for_db."""
        if value and (self.document.get("delete_db", False) or self.document.get("create_db", False)):
            self._error(
                field,
                "wait_for_db == True requires delete_db == False and create_db == False",
            )
        if value and not (self.document.get("create_table", False) or self.document.get("wait_for_table", False)):
            self._error(
                field,
                "wait_for_db == True requires either create_table == True or wait_for_table == True",
            )

    def _check_with_valid_wait_for_table(self, field: str, value: Any) -> None:
        """Validate wait_for_table."""
        if value and (self.document.get("delete_table", False) or self.document.get("create_table", False)):
            self._error(
                field,
                "wait_for_table == True requires delete_table == False and create_table == False",
            )


with open(
    join(dirname(__file__), "formats/raw_table_config_format.json"),
    "r",
    encoding="utf8",
) as file_ptr:
    PYPGTABLE_TABLE_CONFIG_SCHEMA: dict[str, Any] = load(file_ptr)
raw_table_config_validator: _raw_table_config_validator = _raw_table_config_validator(PYPGTABLE_TABLE_CONFIG_SCHEMA, purge_unknown=True)

# Table validators are just aliases of the raw table validators (but not the same object)
table_config_validator: base_validator = raw_table_column_config_validator
table_column_config_validator: base_validator = deepcopy(raw_table_column_config_validator)
