import asyncio
import time
import queue
import functools
import inspect
from typing import Union, List, Literal, Any
import threading


__all__ = ["rate_limit", "rate_limit_async", "RateLimit", "RateLimitAsync", "RateLimitQueue", "RateLimitQueueAsync"]


class RateLimit:
    """
    Thread-safe rate limit for functions.
    """
    def __init__(self, *, rps: int = None, rpm: int = None, limit: int = None, interval: int = None):
        assert bool(rps) + bool(rpm) + bool(limit is not None and interval is not None) == 1, 'Either rps, rpm or limit and interval must be specified.'
        if rps is not None:
            self.limit = rps
            self.interval = 1
        elif rpm is not None:
            self.limit = rpm
            self.interval = 60
        if limit is not None:
            self.limit = limit
            self.interval = interval
        self.created_times = []
        self.lock = threading.Lock()

    def wait_for_rate_limit(self):
        with self.lock:
            while len(self.created_times) == self.limit:
                sleep = self.interval - (time.time() - self.created_times[0])
                if sleep > 0:
                    time.sleep(sleep)
                while self.created_times and self.created_times[0] < time.time() - self.interval:
                    self.created_times.pop(0)
            self.created_times.append(time.time())


class RateLimitQueue:
    def __init__(self, maxsize, *, rps_put: int = None, rpm_put: int = None, rps_get: int = None, rpm_get: int = None, limit_put: int = None, interval_put: float = None, limit_get: float = None, interval_get: float = None):
        self.queue = queue.Queue(maxsize=maxsize)
        if rps_put or rpm_put or limit_put or interval_put:
            self.rate_limit_put = RateLimit(rps=rps_put, rpm=rpm_put, limit=limit_put, interval=interval_put)
        else:
            self.rate_limit_put = None
        
        if rps_get or rpm_get or limit_get or interval_get:
            self.rate_limit_get = RateLimit(rps=rps_get, rpm=rpm_get, limit=limit_get, interval=interval_get)
        else:
            self.rate_limit_get = None

    def get(self):
        if self.rate_limit_get:
            self.rate_limit_get.wait_for_rate_limit()
        return self.queue.get()

    def put(self, item):
        if self.rate_limit_put:
            self.rate_limit_put.wait_for_rate_limit()
        return self.queue.put(item)
    
    def put_nowait(self, item: Any) -> None:
        assert self.rate_limit_put is None, 'put_nowait cannot be used with rate_limit_put.'
        return self.queue.put_nowait(item)


class RateLimitAsync:
    """
    Coroutine-safe rate limit for coroutines.
    """
    def __init__(self, *, rps: int = None, rpm: int = None, limit: int = None, interval: int = None):
        assert bool(rps) + bool(rpm) + bool(limit is not None and interval is not None) == 1, 'Either rps, rpm or limit and interval must be specified.'
        if rps is not None:
            self.limit = rps
            self.interval = 1
        elif rpm is not None:
            self.limit = rpm
            self.interval = 60
        if limit is not None:
            self.limit = limit
            self.interval = interval
        self.created_times = []

        self.current_loop = None
        self.lock = None

    async def wait_for_rate_limit(self):
        if self.current_loop is not asyncio.get_running_loop():
            self.current_loop = asyncio.get_running_loop()
            self.lock = asyncio.Lock()
        async with self.lock:
            while len(self.created_times) == self.limit:
                sleep = self.interval - (time.time() - self.created_times[0])
                if sleep > 0:
                    await asyncio.sleep(sleep)
                while self.created_times and self.created_times[0] < time.time() - self.interval:
                    self.created_times.pop(0)
            self.created_times.append(time.time())


class RateLimitQueueAsync(asyncio.Queue):
    def __init__(self, maxsize, *, rps_put: int = None, rpm_put: int = None, rps_get: int = None, rpm_get: int = None, limit_put: int = None, interval_put: float = None, limit_get: float = None, interval_get: float = None):
        super().__init__(maxsize=maxsize)
        if rps_put or rpm_put or limit_put or interval_put:
            self.rate_limit_put = RateLimitAsync(rps=rps_put, rpm=rpm_put, limit=limit_put, interval=interval_put)
        else:
            self.rate_limit_put = None
        
        if rps_get or rpm_get or limit_get or interval_get:
            self.rate_limit_get = RateLimitAsync(rps=rps_get, rpm=rpm_get, limit=limit_get, interval=interval_get)
        else:
            self.rate_limit_get = None

    async def get(self):
        if self.rate_limit_get:
            await self.rate_limit_get.wait_for_rate_limit()
        return await super().get()

    async def put(self, item):
        if self.rate_limit_put:
            await self.rate_limit_put.wait_for_rate_limit()
        return await super().put(item)
    
    def put_nowait(self, item: Any) -> None:
        assert self.rate_limit_put is None, 'put_nowait cannot be used with rate_limit_put.'
        return super().put_nowait(item)


def rate_limit(rps: int = None, rpm: int = None, limit: int = None, interval: int = None):
    """
    Decorator for rate limiting a function
    """
    def decorator(func):
        r = RateLimit(rps=rps, rpm=rpm, limit=limit, interval=interval)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            r.wait_for_rate_limit()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def rate_limit_async(rps: int = None, rpm: int = None, limit: int = None, interval: int = None):
    """
    Decorator for rate limiting a coroutine.
    """
    def decorator(func):
        r = RateLimitAsync(rps=rps, rpm=rpm, limit=limit, interval=interval)
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            await r.wait_for_rate_limit()
            return await func(*args, **kwargs)
        return wrapper
    return decorator

