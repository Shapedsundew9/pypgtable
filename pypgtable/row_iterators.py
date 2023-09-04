"""Row iterators."""
from collections import namedtuple
from logging import DEBUG, Logger, NullHandler, getLogger
from typing import Any, Callable, Self, Iterable

from psycopg2.extensions import cursor

_logger: Logger = getLogger(__name__)
_logger.addHandler(NullHandler())
_LOG_DEBUG: bool = _logger.isEnabledFor(DEBUG)


class _base_iter:
    """Iterator returning a container of decoded values from values.

    The order of the containers returned is the same as the rows of values in values.
    Each value is decoded by the registered conversion function (see register_conversion()) or
    unchanged if no conversion has been registered.
    """

    def __init__(self, columns: Iterable[str], values, _table, code: str = "decode") -> None:
        """Initialise.

        Args
        ----
        columns (iter(str)): Column names for each of the rows in values.
        values  (row_iter): Iterator over rows (tuples) with values in the order as columns.
        """
        self.values = values
        self.conversions: list[Callable[[Any], Any] | None] = [_table._conversions[column][code] for column in columns]
        self.columns: Iterable[str] = columns

    def __iter__(self) -> Self:
        """Self iteration."""
        return self

    def __next__(self) -> Any:
        """Never gets run."""
        raise NotImplementedError

    def __del__(self) -> None:
        if isinstance(self.values, cursor):
            _logger.debug("Closing held DB cursor.")
            self.values.close()


class gen_iter(_base_iter):
    """Iterator returning a generator for decoded values from values."""

    # FIXME: Forced to type hint 'Any' as pylance unable to work out which iterator is returned.
    def __next__(self) -> Any:
        """Return next value."""
        return (v if f is None else f(v) for f, v in zip(self.conversions, next(self.values)))


class tuple_iter(_base_iter):
    """Iterator returning a tuple for decoded values from values."""

    # FIXME: Forced to type hint 'Any' as pylance unable to work out which iterator is returned.
    def __next__(self) -> Any:
        """Return next value."""
        return tuple((v if f is None else f(v) for f, v in zip(self.conversions, next(self.values))))


class namedtuple_iter(_base_iter):
    """Iterator returning a namedtuple for decoded values from values."""

    def __init__(self, columns: Iterable[str], values, _table, code: str = "decode") -> None:
        super().__init__(columns, values, _table, code)
        self.namedtuple = namedtuple("row", columns)

    # FIXME: Forced to type hint 'Any' as pylance unable to work out which iterator is returned.
    def __next__(self) -> Any:
        """Return next value."""
        return self.namedtuple((v if f is None else f(v) for f, v in zip(self.conversions, next(self.values))))


class dict_iter(_base_iter):
    """Iterator returning a dict for decoded values from values."""

    # FIXME: Forced to type hint 'Any' as pylance unable to work out which iterator is returned.
    def __next__(self) -> Any:
        """Return next value."""
        return {c: v if f is None else f(v) for c, f, v in zip(self.columns, self.conversions, next(self.values))}
