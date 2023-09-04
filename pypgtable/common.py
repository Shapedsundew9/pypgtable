"""Common functions for database."""


from random import random
from typing import Any, Callable, Generator

from .pypgtable_typing import DatabaseConfigNorm


def backoff_generator(initial_delay: float = 0.125, backoff_steps: int = 13, fuzz: bool = True) -> Generator[Any, None, None]:
    """Generate increasing connection retry attempt delays.

    Increase delay by a factor of two each time until maximum delay is reached.
    If fuzz is true delay is increased by a random value in the range -0.1*delay to 0.1*delay
    The maximum delay is repeated infinitely.

    Args
    ----
    initial_delay: 1st backoff delay in seconds.
    backoff_steps: >=0 number of times to double delay before saturating.
    fuzz: If true +/-10% fuzz factor to each delay

    Returns
    -------
    (float): Delay in seconds.
    """
    fuzz_func: Callable[..., Any] = (lambda x: (1 + 0.2 * (random() - 0.5)) * x) if fuzz else lambda x: x
    for backoff in (initial_delay * 2**n for n in range(backoff_steps)):
        yield fuzz_func(backoff)
    while True:
        yield fuzz_func(initial_delay * 2**backoff_steps)


def connection_str_from_config(db_config: DatabaseConfigNorm, with_password: bool = False) -> str:
    """Create a posgresl connection string from a DB configutation.

    Args
    ----
    db_config: Normalized DB configuration.
    with_password: Include the password in the string if True.

    Returns
    -------
    A postgresql connection string.
    postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
    """
    connection_str: str = "postgresql://"
    connection_str += db_config["user"]
    if with_password and db_config["password"] is not None:
        connection_str += ":" + db_config["password"]
    connection_str += "@" + db_config["host"]
    connection_str += ":" + str(db_config["port"])
    connection_str += "/" + db_config["dbname"]
    return connection_str
