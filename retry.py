import functools
import asyncio
import logging


__all__ = ['retry', 'safe']



def safe(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            return None
    return wrapper


def retry(max_retry: int = 5, exception = Exception, cooldown: float = 0):
    """
    Decorator for retrying a function / coroutine.
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for _ in range(max_retry):
                try:
                    return await func(*args, **kwargs)
                except exception as e:
                    ex = e
                if cooldown > 0:
                    await asyncio.sleep(cooldown)
            raise ex
        return wrapper
    return decorator