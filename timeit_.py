import time
from typing import *
import inspect


__all__ = ["timeit"]


def timeit(func: Callable):
    """
    Decorator for timing a function / coroutine.
    """
    if inspect.iscoroutinefunction(func):
        async def wrapper(*args, **kwargs):
            start = time.time()
            ret = await func(*args, **kwargs)
            end = time.time()
            print(f"{func.__qualname__} took {end - start} seconds.")
            return ret
        return wrapper
    else:
        def wrapper(*args, **kwargs):
            start = time.time()
            ret = func(*args, **kwargs)
            end = time.time()
            print(f"{func.__qualname__} took {end - start} seconds.")
            return ret
        return wrapper