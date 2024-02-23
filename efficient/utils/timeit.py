import time
from typing import *
import inspect


__all__ = ["timeit"]


class timeit:
    def __init__(self, name: str = None):
        self.name = name
        
    def __call__(self, func: Callable):
        if inspect.iscoroutinefunction(func):
            async def wrapper(*args, **kwargs):
                with timeit(self.name or func.__qualname__):
                    ret = await func(*args, **kwargs)
                return ret
            return wrapper
        else:
            def wrapper(*args, **kwargs):
                with timeit(self.name or func.__qualname__):
                    ret = func(*args, **kwargs)
                return ret
            return wrapper
    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        print(f"{self.name} took {end - self.start} seconds.")