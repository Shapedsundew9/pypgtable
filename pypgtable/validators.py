"""Validators for database."""


from copy import deepcopy
from cerberus import Validator
from json import load
from os.path import dirname, join
from .utils.base_validator import BaseValidator


with open(join(dirname(__file__), "formats/database_config_format.json"), "r") as file_ptr:
    database_config_validator = Validator(load(file_ptr))
with open(join(dirname(__file__), "formats/raw_table_column_config_format.json"), "r") as file_ptr:
    raw_table_column_config_validator = Validator(load(file_ptr))


class _raw_table_config_validator(BaseValidator):

    def sub_normalized(self, document):
        """Normalize sub-documents."""
        document = deepcopy(document)
        document['database'] = database_config_validator.normalized(document['database'])
        if 'schema' in document:
            for column in document['schema']:
                document['schema'][column] = raw_table_column_config_validator.normalized(document['schema'][column])
        return self.normalized(document)

    def _check_with_valid_database_config(self, field, value):
        """Validate database configuration."""
        if not database_config_validator.validate(value):
            for e in database_config_validator._errors:
                self._error(field, self.str_errors(e))

    def _check_with_valid_raw_table_column_config(self, field, value):
        """Validate every column configuration."""
        if not raw_table_column_config_validator.validate(value):
            for e in raw_table_column_config_validator._errors:
                self._error(field, self.str_errors(e))
        if value.get('nullable', False) and value.get('primary_key', False):
            self._error(field, 'A column cannot be both NULL and the PRIMARY KEY.')
        if value.get('unique', False) and value.get('primary_key', False):
            self._error(field, 'A column cannot be both UNIQUE and the PRIMARY KEY.')

    def _check_with_valid_schema_config(self, field, value):
        """Validate the overall schema. There can be only one primary key."""
        primary_key_count = sum((config.get('primary_key', False) for config in value.values()))
        if primary_key_count > 1:
            self._error(field, "There are {} primary keys defined. There can only be 0 or 1.".format(primary_key_count))

    def _check_with_valid_ptr_map_config(self, field, value):
        """Validate pointer map configuration."""
        for k, v in value.items():
            if v in value.keys():
                self._error(field, "Circular reference {} -> {}".format(v, value[v]))
            if k not in self.document['schema'].keys():
                self._error(field, "Key {} is not a field.".format(k))
            if v not in self.document['schema'].keys():
                self._error(field, "Value {} is not a field.".format(v))

    def _check_with_valid_file_folder(self, field, value):
        """Validate data file folder exist if validate is set."""
        self._isdir(field, value)

    def _check_with_valid_data_files(self, field, value):
        """Validate the data files if validate is set."""
        for filename in value:
            abspath = join(self.document['data_file_folder'], filename)
            if not self._isjsonfile(field, abspath):
                self._error(field, "Data file {} empty or invalid.".format(abspath))

    def _check_with_valid_delete_db(self, field, value):
        """Validate delete_db."""
        if value and not self.document.get('create_db', False):
            self._error(field, "delete_db == True requires create_db == True")

    def _check_with_valid_delete_table(self, field, value):
        """Validate delete_table."""
        if value and not self.document.get('create_table', False):
            self._error(field, "delete_table == True requires create_table == True")


with open(join(dirname(__file__), "formats/raw_table_config_format.json"), "r") as file_ptr:
    raw_table_config_validator = _raw_table_config_validator(load(file_ptr))
