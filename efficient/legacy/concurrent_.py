import asyncio
import functools
from typing import *
import inspect

__all__ = ["concurrent_limit", "concurrent_async", "concurrent_thread"]


def concurrent_limit(max_concurrency: int) -> Coroutine:
    def decorator(fn: Callable):
        if inspect.iscoroutinefunction(fn):
            semaphore = asyncio.Semaphore(max_concurrency)
            @functools.wraps(fn)
            async def wrapper(*args, **kwargs):
                async with semaphore:
                    return await fn(*args, **kwargs)
        else:
            semaphore = threading.Semaphore(max_concurrency)
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                with semaphore:
                    return fn(*args, **kwargs)
        return wrapper
    return decorator


class AutoSemaphore:
    def __init__(self, value) -> None:
        self.semaphore = None
        self.current_loop = None
        self.value = value

    def get(self) -> asyncio.Semaphore:
        if self.current_loop is not asyncio.get_running_loop():
            self.current_loop = asyncio.get_running_loop()
            self.semaphore = asyncio.Semaphore(self.value)      

        return self.semaphore  

def concurrent_async(limit: int = None):
    def decorator(func):
        if limit:
            semaphore = AutoSemaphore(limit)
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if limit:
                async def task():
                    ret = await func(*args, **kwargs)
                    semaphore.get().release()
                    return ret
                await semaphore.get().acquire()
                return asyncio.create_task(task())
            else:
                return asyncio.create_task(func(*args, **kwargs))
        return wrapper
    return decorator


from concurrent.futures import ThreadPoolExecutor
import threading


def concurrent_thread(limit: int = None):
    def decorator(func):
        if limit:
            semaphore = threading.Semaphore(limit)
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