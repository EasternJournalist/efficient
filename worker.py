import traceback
from functools import wraps, partial
import time
import asyncio
from typing import * 

import multiprocessing as mp
from multiprocessing.connection import Connection

__all__ = ['AsyncWorker', 'AsyncWorkers', 'async_worker', 'async_workers']


class AsyncWorker:
    """
    A concurrnent function wrapper. A PERSISTENT worker process is created to loop over the function, query inputs and return outputs.
    The benifit is that the worker process is not created and destroyed
    every time the function is called. This is useful when the function refers heavy data.
    """
    def __init__(self, func: Callable, verbose: bool = False):
        """
        Parameters
        ----------
        func: a picklable function
        trace_footprints: bool: whether to trace the time footprints of the function
        """
        self.func = func
        self.verbose = verbose
        self.parent_conn, self.child_conn = mp.Pipe(duplex=True)
        self.process = mp.Process(target=self._process, args=(self.child_conn,))
        self.process.start()

    def _process(self, conn: Connection):
        if self.verbose:
            print("Process for \"{}\" is created".format(str(self.func)))
        try:
            while True:
                args, kwds = conn.recv()
                if self.verbose:
                    print("Process for \"{}\" is called".format(str(self.func)))
                output = self.func(*args, **kwds)
                conn.send(output)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Error occurred in worker process for \"{str(self.func)}\"")
            traceback.print_exc()
            print('Exception: {}'.format(e))
            
    async def __call__(self, *args, **kwds):
        self.parent_conn.send((args, kwds))
        data_available = asyncio.Event()
        asyncio.get_event_loop().add_reader(self.parent_conn.fileno(), data_available.set)

        if not self.parent_conn.poll():
            await data_available.wait()

        result = self.parent_conn.recv()
        data_available.clear()
        return result

    def __del__(self):
        if hasattr(self, 'process'):
            self.process.terminate()
            self.process.join()


class AsyncWorkers:
    def __init__(self, func: Callable, num_workers: int):
        self.func = func
        self.num_workers = num_workers
        self.workers = [AsyncWorker(wraps(func)(partial(func, worker_id=i))) for i in range(num_workers)]
        self.free_workers: asyncio.Queue = None
        self.current_loop = None

    async def __call__(self, *args, **kwds):
        if self.current_loop is not asyncio.get_running_loop():
            self.current_loop = asyncio.get_running_loop()
            self.free_workers = asyncio.Queue(maxsize=self.num_workers)
            for worker in self.workers:
                self.free_workers.put_nowait(worker)
        worker: AsyncWorker = await self.free_workers.get()
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


def async_worker(unbounded_method: Callable):
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
                setattr(self, fname, AsyncWorker(bound_method))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return wraps(unbounded_method)(AsyncWorker(unbounded_method))
    

@decorator_with_optional_args
def async_workers(unbounded_method: Callable, *, num_workers: int):
    """
    A decorator that wraps a method as a number of workers running separate processes respectively.
    Jobs will be submitted to a free worker process. If all workers are busy, the main process will be blocked.
    The processes will be created when the method is called for the first time. 
    The wrapped method will return an Result object, which can be used to get the result.
    
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
                setattr(self, fname, AsyncWorkers(bound_method, num_workers))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return wraps(unbounded_method)(AsyncWorkers(unbounded_method, num_workers))