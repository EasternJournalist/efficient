import functools
import asyncio
import logging
import inspect
import time
from typing import *


__all__ = ["safe", "retry"]


def safe(default: Any = None):
    """
    Decorator for catching exceptions in a function / coroutin, and returns default value.
    """
    def decorator(func: Callable):
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    logging.exception(e)
                    return default
            return wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.exception(e)
                    return default
            return wrapper
    return decorator


def retry(max_retry: int = 5, exception = Exception, cooldown: float = 0):
    """
    Decorator for retrying a function / coroutine.
    """
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                for i in range(max_retry):
                    try:
                        return await func(*args, **kwargs)
                    except exception as e:
                        if i == max_retry - 1:
                            raise
                    if cooldown > 0:
                        await asyncio.sleep(cooldown)
            return wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                for i in range(max_retry):
                    try:
                        return func(*args, **kwargs)
                    except exception as e:
                        if i == max_retry - 1:
                            raise
                    if cooldown > 0:
                        time.sleep(cooldown)
            return wrapper
    return decorator