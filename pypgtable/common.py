"""Common functions for database."""


from random import random


def backoff_generator(initial_delay=0.125, backoff_steps=13, fuzz=True):
    """Generate increasing connection retry attempt delays.

    Increase delay by a factor of two each time until maximum delay is reached.
    If fuzz is true delay is increased by a random value in the range -0.1*delay to 0.1*delay
    The maximum delay is repeated infinitely.

    Args
    ----
    initial_delay (float): 1st backoff delay in seconds.
    backoff_steps (int): >=0 number of times to double delay before saturating.
    fuzz (bool): If true +/-10% fuzz factor to each delay

    Returns
    -------
    (float): Delay in seconds.
    """
    fuzz_func = (lambda x: (1 + 0.2 * (random() - 0.5)) * x) if fuzz else lambda x: x
    for backoff in (initial_delay * 2**n for n in range(backoff_steps)):
        yield fuzz_func(backoff)
    while True:
        yield fuzz_func(initial_delay * 2**backoff_steps)
