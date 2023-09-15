import asyncio
from typing import *
from .concurrent_ import concurrent_limit

__all__ = ["aiomap"]


async def aiomap(fn: Coroutine, iterable: Iterable, max_concurrency: int = None):
    if max_concurrency is not None:
        fn_ = concurrent_limit(max_concurrency)(fn)
    else:
        fn_ = fn
    return await asyncio.gather(*(asyncio.create_task(fn_(arg)) for arg in iterable))