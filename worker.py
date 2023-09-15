import traceback
from functools import wraps, partial
import time
import asyncio
from typing import * 
import multiprocessing as mp
from multiprocessing.connection import Connection
import pickle

from aiopipe import aioduplex, AioDuplex

__all__ = ['AsyncWorker', 'AsyncWorkers', 'async_worker', 'async_workers']


class AsyncWorker:
    """
    A concurrnent function wrapper. A PERSISTENT worker process is created to loop over the function, query inputs and return outputs.
    The benifit is that the worker process is not created and destroyed
    every time the function is called. This is useful when the function refers heavy data.
    """
    def __init__(self, func: Callable, verbose: bool = False, revive: bool = False):
        """
        Parameters
        ----------
        func: a picklable function
        """
        self.func = func
        self.verbose = verbose
        self.revive = revive
        self.init_process()
    
    def init_process(self):
        self.parent_conn, self.child_conn = aioduplex()
        with self.child_conn.detach() as child_conn:
            self.process = mp.Process(target=self._process, args=(child_conn,))
        self.process.start()

    def _process(self, conn: AioDuplex):
        async def _task():
            print('start')
            if self.verbose:
                print("Process for \"{}\" is created".format(str(self.func)))
            try:
                while True:
                    async with conn.open() as (reader, writer):
                        print('fuck')
                        args, kwds = pickle.loads(await reader.read())
                        print('fuck then')
                        if self.verbose:
                            print("Process for \"{}\" is called".format(str(self.func)))
                        output = self.func(*args, **kwds)
                        print('send')
                        writer.write(pickle.dumps(output))
                    print('sended')
            except KeyboardInterrupt:
                pass
            except Exception as e:
                print(f"Error occurred in worker process for \"{str(self.func)}\"")
                traceback.print_exc()
                print('Exception: {}'.format(e))
        asyncio.run(_task())
            
    async def __call__(self, *args, **kwds):
        if not self.process.is_alive():
            assert self.revive, f"Worker process for \"{str(self.func)}\" is dead. Set revive=True to allow restart"
            if self.verbose:
                print(f"Revive process for \"{str(self.func)}\"")
            self.init_process()
        # self.parent_conn.send((args, kwds))
        # data_available = asyncio.Event()
        # asyncio.get_event_loop().add_reader(self.parent_conn.fileno(), data_available.set)
        # while True:
        #     try:
        #         print('wait')
        #         await asyncio.wait_for(data_available.wait(), timeout=5)
        #         print('got')
        #         break
        #     except asyncio.exceptions.TimeoutError:
        #         print('wait timeout')
        #         pass
        #     if not self.process.is_alive():
        #         self.process.kill()    
        #         raise RuntimeError(f"Worker process for \"{str(self.func)}\" is dead")
        # print('get')
        # result = self.parent_conn.recv()
        # print('got')
        async with self.parent_conn.open() as (reader, writer):
            writer.write(pickle.dumps((args, kwds)))
            while True:
                try:
                    result = await asyncio.wait_for(reader.read(), timeout=5)
                    if not result:
                        continue
                    print('got', result)
                    result = pickle.loads(result)
                    return result
                except asyncio.exceptions.TimeoutError:
                    print('wait timeout')
                    pass
                if not self.process.is_alive():
                    self.process.kill()    
                    raise RuntimeError(f"Worker process for \"{str(self.func)}\" is dead")

    def __del__(self):
        if hasattr(self, 'process'):
            self.process.terminate()
            self.process.join()


class AsyncWorkers:
    def __init__(self, func: Callable, num_workers: int, verbose: bool = False, revive: bool = False):
        self.func = func
        self.num_workers = num_workers
        self.revive = revive
        self.workers = [
            AsyncWorker(wraps(func)(partial(func, worker_id=i)), verbose=verbose, revive=revive) 
                for i in range(num_workers)
        ]
        self.free_workers: asyncio.Queue = None
        self.current_loop = None

    async def __call__(self, *args, **kwds):
        if self.current_loop is not asyncio.get_running_loop():
            self.current_loop = asyncio.get_running_loop()
            self.lock = asyncio.Lock()
            self.free_workers = asyncio.Queue(maxsize=self.num_workers)
            for worker in self.workers:
                self.free_workers.put_nowait(worker)
        worker: AsyncWorker = await self.free_workers.get()
        if self.revive:
            try:
                ret = await worker(*args, **kwds)
            except RuntimeError:
                self.free_workers.put_nowait(worker)
                raise
        else:
            ret = await worker(*args, **kwds)
        self.free_workers.put_nowait(worker)
        return ret


def decorator_with_optional_args(decorator):
    """
    This decorator allows a decorator to be used with or without arguments.
    """
    @wraps(decorator)
    def new_decorator(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            return decorator(args[0])
        else:
            return lambda real_func: decorator(real_func, *args, **kwargs)
    return new_decorator


@decorator_with_optional_args
def async_worker(unbounded_method: Callable, *, verbose: bool = False, revive: bool = False):
    """
    A decorator that wraps a method as a worker method running in a persistent separate process.
    The process will be created when the method is called for the first time. 
    The wrapped method becomes a Coroutine.
    
    NOTE: This decorator wrap the method only in the main process. In subprocesses, this decorator does nothing but return the original method.
    """
    if mp.current_process().name != 'MainProcess':
        return unbounded_method
    elif '.' in unbounded_method.__qualname__:
        # For class method
        fname = str(unbounded_method.__name__)
        @wraps(unbounded_method)
        def wrapper2(self, *args, **kwds):
            if not isinstance(getattr(self, fname), AsyncWorker):
                bound_method = unbounded_method.__get__(self)
                setattr(self, fname, AsyncWorker(bound_method, verbose=verbose, revive=revive))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return wraps(unbounded_method)(AsyncWorker(unbounded_method, verbose=verbose, revive=revive))
    

@decorator_with_optional_args
def async_workers(unbounded_method: Callable, *, num_workers: int, verbose: bool = False, revive: bool = False):
    """
    A decorator that wraps a method as a number of workers running separate processes respectively.
    Jobs will be submitted to a free worker process. If all workers are busy, the main process will be blocked.
    The processes will be created when the method is called for the first time. 
    The wrapped method becomes a Coroutine.
    
    NOTE: This decorator wrap the method only in the main process. In subprocesses, this decorator does nothing but return the original method.
    """
    if mp.current_process().name != 'MainProcess':
        return unbounded_method
    elif '.' in unbounded_method.__qualname__:
        # For class method
        fname = str(unbounded_method.__name__)
        @wraps(unbounded_method)
        def wrapper2(self, *args, **kwds):
            if not isinstance(getattr(self, fname), AsyncWorkers):
                bound_method = unbounded_method.__get__(self)
                setattr(self, fname, AsyncWorkers(bound_method, num_workers=num_workers, verbose=verbose, revive=revive))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return wraps(unbounded_method)(AsyncWorkers(unbounded_method, num_workers=num_workers, verbose=verbose, revive=revive))