import asyncio
from typing import *

__all__ = ["aiomap"]


async def aiomap(fn: Coroutine, iterable: Iterable, max_concurrency: int = None):
    if max_concurrency is not None:
        fn_ = concurrency_limit(fn, max_concurrency)
    else:
        fn_ = fn
    return await asyncio.gather(*(asyncio.create_task(fn_(arg)) for arg in iterable))