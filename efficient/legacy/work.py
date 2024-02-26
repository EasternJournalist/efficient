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