import asyncio
import functools

def concurrent_async(limit: int = None):
    def decorator(func):
        if limit:
            semaphore = asyncio.Semaphore(limit)
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if limit:
                async with semaphore:
                    return asyncio.create_task(func(*args, **kwargs))
            else:
                return asyncio.create_task(func(*args, **kwargs))
        return wrapper
    return decorator


from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore 


def concurrent_thread(limit: int = None):
    def decorator(func):
        if limit:
            semaphore = Semaphore(limit)
        thread_pool_executor = ThreadPoolExecutor(max_workers=limit)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if limit:
                with semaphore:
                    return thread_pool_executor.submit(func, *args, **kwargs)
            else:
                return thread_pool_executor.submit(func, *args, **kwargs)
        return wrapper
    return decorator