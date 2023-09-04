"""Extension to the Cerberus Validator with common checks."""

from json import JSONDecodeError, load
from logging import Logger, NullHandler, getLogger
from os import R_OK, W_OK, X_OK, access
from os.path import isdir, isfile
from pprint import pformat
from typing import Any, Callable
from uuid import UUID

from cerberus import TypeDefinition, Validator
from cerberus.errors import UNKNOWN_FIELD

_logger: Logger = getLogger(__name__)
_logger.addHandler(NullHandler())


class base_validator(Validator):
    """Additional format checks."""

    types_mapping: Any = Validator.types_mapping.copy()  # type: ignore
    types_mapping["uuid"] = TypeDefinition("uuid", (UUID,), ())
    types_mapping["callable"] = TypeDefinition("callable", (Callable,), ())

    def __init__(self, *args, **kwargs) -> None:
        # FIXME: To satisfy pylance.
        # FIXME: Could better define types here.
        # Cerberus does some complex dynamic definition that pylance cannot statically resolve
        self.document: Any = None
        super().__init__(*args, **kwargs)
        self._error: Callable[[str, str], None] = super()._error  # type: ignore
        self.schema: Any = super().schema  # type: ignore
        self.normalized: Callable = super().normalized  # type: ignore
        self.validate: Callable = super().validate  # type: ignore

    def error_str(self) -> str:
        """Prettier format to a list of errors."""
        return "\n".join((field + ": " + pformat(error) for field, error in self.errors.items()))  # type: ignore

    def _isdir(self, field: str, value: Any) -> bool:
        """Validate value is a valid, existing directory."""
        if not isdir(value):
            self._error(field, f"{value} is not a valid directory or does not exist.")
            return False
        return True

    def _isfile(self, field: str, value: Any) -> bool:
        """Validate value is a valid, existing file."""
        if not isfile(value):
            self._error(field, f"{value} is not a valid file or does not exist.")
            return False
        return True

    def _isreadable(self, field: str, value: Any) -> bool:
        """Validate value is a readable file."""
        if not access(value, R_OK):
            self._error(field, f"{value} is not readable.")
            return False
        return True

    def _iswriteable(self, field: str, value: Any) -> bool:
        """Validate value is a writeable file."""
        if not access(value, W_OK):
            self._error(field, f"{value} is not writeable.")
            return False
        return True

    def _isexecutable(self, field: str, value: Any) -> bool:
        """Validate value is an executable file."""
        if not access(value, X_OK):
            self._error(field, f"{value} is not executable.")
            return False
        return True

    def _isjsonfile(self, field: str, value: Any) -> dict | list | None:
        """Validate the JSON file is decodable."""
        if self._isfile(field, value) and self._isreadable(field, value):
            with open(value, "r", encoding="utf8") as file_ptr:
                try:
                    schema: dict | list = load(file_ptr)
                except JSONDecodeError as exception:
                    self._error(field, f"The file is not decodable JSON: {exception}")
                else:
                    return schema
        return None

    def str_errors(self, error: Any) -> str:
        """Create an error string."""
        if error.code == UNKNOWN_FIELD.code:
            error.rule = "unknown field"
        str_tuple: tuple[str, str, str] = ("Value: " + str(error.value), "Rule: " + str(error.rule), "Constraint: " + str(error.constraint))
        return ", ".join(str_tuple)
