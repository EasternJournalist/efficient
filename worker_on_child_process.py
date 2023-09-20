import traceback
from functools import wraps, partial
from dataclasses import dataclass
import time
import asyncio
from typing import * 
import multiprocessing as mp
from multiprocessing.connection import Connection
import pickle
from abc import abstractmethod, ABC


__all__ = ['Scheduler', 'WorkerOnChildProcess', 'WorkersOnChildProcesses', 'worker_on_child_process', 'workers_on_child_processes']



class WorkerOnChildProcess(ABC):
    """
    Inherit this class to create a worker process that loops over `.work(...)` function as a coroutine asynchronously.
    The benifit is that the worker process is not created and destroyed. (This is efficient when the function refers heavy data.)
    """
    CHECK_ALIVE_INTERVAL = 3

    def __init__(self, verbose: bool = False, revive: bool = False):
        """
        Args:
            verbose: Whether to print verbose information.
            revive: Whether to revive the worker process when it is dead.
        """
        self.verbose = verbose
        self.revive = revive
        self.child_process: mp.Process = None

    def start(self):
        if self.child_process is not None:
            if not self.revive:
                raise RuntimeError(f"Worker process is already started. Set revive=True to allow restarting child process")
            self.terminate()
            self.read, self.write, self.child_process = None, None, None
            print("Restart worker process")
        read, child_write = mp.Pipe(duplex=False)
        child_read, write = mp.Pipe(duplex=False)
        child_process = mp.Process(target=self._child_process, args=(child_read, child_write))
        child_process.start()
        self.read, self.write, self.child_process = read, write, child_process

    def _child_process(self, child_read: Connection, child_write: Connection):
        if self.verbose:
            print("Worker process is created")
        try:
            self.init()
            while True:
                args, kwargs = child_read.recv()
                if self.verbose:
                    print("Worker process is called")
                output = self.work(*args, **kwargs)
                child_write.send(output)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"\033[91mError occurred in worker process \033[0m")
            traceback.print_exc()

    @abstractmethod
    def init(self):
        ...

    @abstractmethod
    def work(self, *args, **kwargs):
        ...

    async def run(self, *args, **kwds):
        if self.child_process is None or not self.child_process.is_alive():
            self.start()

        self.write.send((args, kwds))
        data_available = asyncio.Event()
        asyncio.get_event_loop().add_reader(self.read.fileno(), data_available.set)
        while True:
            try:
                await asyncio.wait_for(data_available.wait(), timeout=self.CHECK_ALIVE_INTERVAL)
            except asyncio.exceptions.TimeoutError:
                pass
            if not self.child_process.is_alive():   
                raise RuntimeError(f"Worker process died")
            if self.read.poll():
                break
        result = self.read.recv()
        data_available.clear()
        asyncio.get_event_loop().remove_reader(self.read.fileno())
        return result

    def terminate(self):
        self.child_process.terminate()
        self.child_process.join()

    def kill(self):
        self.child_process.kill()
        self.child_process.join()
    
    def __del__(self):
        self.terminate()


class Scheduler:
    """
    A scheduler that schedule resources to run. 
    Each resource can only be used by one run at a time.
    After the function is finished, the resource is returned to the scheduler and free to be used by for next run.
    """
    def __init__(self, resources: List[Any], recycle_on_exception: bool = False):
        self.resources = resources
        self.recycle_on_exception = recycle_on_exception
        self.free_objects = asyncio.Queue(maxsize=len(resources))
        for resource in resources:
            self.free_objects.put_nowait(resource)

    async def run(self, fn: Callable[[Any], Awaitable[Any]]):
        resource = await self.free_objects.get()
        if self.recycle_on_exception:
            try:
                result = await fn(resource)
            except:
                self.free_objects.put_nowait(resource)
                raise
        else:
            result = await fn(resource)
        self.free_objects.put_nowait(resource)
        return result


@dataclass
class _WorkerProcess:
    rank: int
    read: Connection
    write: Connection
    process: mp.Process


class WorkersOnChildProcesses(ABC):
    """
    Inherit this class to create a number of worker processes that loop over `.work(...)` function, working in parallel and being scheduled.

    Use `self.RANK` to get the rank of the current worker process.
    """
    CHECK_ALIVE_INTERVAL = 3
    RANK: int = None

    def __init__(self, num_workers: int, verbose: bool = False, revive: bool = False):
        self.num_workers = num_workers
        self.verbose = verbose
        self.revive = revive
        self.workers: List[_WorkerProcess] = None
    
    def _restart(self, worker: _WorkerProcess):
        worker.read, child_write = mp.Pipe(duplex=False)
        child_read, worker.write = mp.Pipe(duplex=False)
        workers, scheduler = self.workers, self.scheduler
        self.workers, self.scheduler = None, None
        worker.process = mp.Process(target=self._child_process, args=(worker.rank, child_read, child_write))
        worker.process.start()
        self.workers, self.scheduler = workers, scheduler

    def start(self):
        if self.workers is not None:
            if not self.revive:
                raise RuntimeError(f"Worker processes are already started. Set revive=True to allow restarting child process")
            for worker in self.workers:
                worker.process.terminate()
                worker.process.join()
            self.workers = None
            if self.verbose:
                print("Restart worker processes")
        workers = []
        for rank in range(self.num_workers):
            read, child_write = mp.Pipe(duplex=False)
            child_read, write = mp.Pipe(duplex=False)
            child_process = mp.Process(target=self._child_process, args=(rank, child_read, child_write))
            child_process.start()
            workers.append(_WorkerProcess(rank, read, write, child_process))
        self.workers = workers
        self.scheduler = Scheduler(workers, recycle_on_exception=self.revive)

    def _child_process(self, rank: int, child_read: Connection, child_write: Connection):
        if self.verbose:
            print("Worker process is created")
        try:
            self.RANK = rank
            self.init()
            while True:
                args, kwargs = child_read.recv()
                if self.verbose:
                    print("Worker process is called")
                output = self.work(*args, **kwargs)
                child_write.send(output)
        except KeyboardInterrupt:
            pass
        except Exception:
            print(f"\033[91mError occurred in worker process rank {rank}\033[0m")
            traceback.print_exc()
        exit()

    @abstractmethod
    def init(self):
        ...

    @abstractmethod
    def work(self, *args, **kwargs):
        ...

    async def run(self, *args, **kwds):
        if self.workers is None:
            self.start()
        async def _task(worker: _WorkerProcess):
            if not worker.process.is_alive():
                assert self.revive, f"Worker process is dead. Set revive=True to allow restarting child process"
                if self.verbose:
                    print(f"Revive worker process")
                self._restart(worker)
            worker.write.send((args, kwds))
            data_available = asyncio.Event()
            asyncio.get_event_loop().add_reader(worker.read.fileno(), data_available.set)
            while True:
                try:
                    if not worker.read.poll():
                        await asyncio.wait_for(data_available.wait(), timeout=self.CHECK_ALIVE_INTERVAL)
                except asyncio.exceptions.TimeoutError:
                    pass
                if not worker.process.is_alive():
                    if self.revive:
                        self._restart(worker)
                    raise RuntimeError(f"Worker process died")
                if worker.read.poll():
                    break
            try:
                result = worker.read.recv()
            except:
                if worker.process.is_alive():
                    worker.process.kill()
                self._restart(worker)
                raise RuntimeError(f"Worker process is corrupted. Kill it")
            asyncio.get_event_loop().remove_reader(worker.read.fileno())
            return result
        return await self.scheduler.run(_task)

    def terminate(self):
        for worker in self.workers:
            if worker.process.is_alive():
                worker.process.terminate()
                worker.process.join()

    def kill(self):
        for worker in self.workers:
            worker.process.kill()
            worker.process.join()
    
    def __del__(self):
        self.terminate()




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


class FunctionWorkerWrapper(WorkerOnChildProcess):
    def __init__(self, fn: Callable, verbose: bool = False, revive: bool = False):
        super().__init__(verbose=verbose, revive=revive)
        self.fn = fn

    def init(self):
        pass

    def work(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    async def __call__(self, *args, **kwargs):
        return await self.run(*args, **kwargs)


@decorator_with_optional_args
def worker_on_child_process(unbounded_method: Callable, *, verbose: bool = False, revive: bool = False):
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
            if not isinstance(getattr(self, fname), FunctionWorkerWrapper):
                bound_method = unbounded_method.__get__(self)
                setattr(self, fname, FunctionWorkerWrapper(bound_method, verbose=verbose, revive=revive))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return wraps(unbounded_method)(FunctionWorkerWrapper(unbounded_method, verbose=verbose, revive=revive))


class FunctionWorkersWrapper(WorkersOnChildProcesses):
    def __init__(self, fn: Callable, num_workers: int, verbose: bool = False, revive: bool = False):
        super().__init__(num_workers, verbose=verbose, revive=revive)
        self.fn = fn
    
    def init(self):
        pass

    def work(self, *args, **kwargs):
        return self.fn(self.RANK, *args, **kwargs)

    async def __call__(self, *args, **kwargs):
        return await self.run(*args, **kwargs)


@decorator_with_optional_args
def workers_on_child_processes(unbounded_method: Callable, *, num_workers: int, verbose: bool = False, revive: bool = False):
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
            if not isinstance(getattr(self, fname), FunctionWorkersWrapper):
                bound_method = unbounded_method.__get__(self)
                setattr(self, fname, FunctionWorkersWrapper(bound_method, num_workers, verbose=verbose, revive=revive))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        wrapper = FunctionWorkersWrapper(unbounded_method, num_workers, verbose=verbose, revive=revive)
        @wraps(unbounded_method)
        def wrapper1(*args, **kwargs):
            return wrapper(*args, **kwargs)
        wrapper.fn = wrapper1
        return wrapper1