import traceback
from functools import wraps
import time

import multiprocessing as mp


class PersistentProcessWorker:
    """
    A concurrnent function wrapper. A PERSISTENT worker process is created to loop over the function, query inputs and return outputs.
    The benifit is that the worker process is not created and destroyed
    every time the function is called. This is useful when the function refers heavy data.
    """
    def __init__(self, func, *, share_arrays: bool = False, trace_footprints: bool = False):
        """
        Parameters
        ----------
        func: a picklable function
        trace_footprints: bool: whether to trace the time footprints of the function
        """
        self.func = func
        self.parent_conn, self.child_conn = Pipe(duplex=True)
        self._footprints = Queue() if trace_footprints else None
        self.share_arrays = share_arrays

    def _process(self, conn, footprints: Queue):
        print("Process for \"{}\" is created".format(self.func.__qualname__))
        try:
            while True:
                args, kwds = conn.recv()
                if footprints is not None:
                    start_t = time.time()
                if self.share_arrays:
                    args, kwds = self.from_shared(args), self.from_shared(kwds)
                output = self.func(*args, **kwds)
                if self.share_arrays:
                    output = self.as_shared(output)

                conn.send(output)
                if footprints is not None:
                    end_t = time.time()
                    footprints.put((start_t, end_t))
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Error occurred in worker process for \"{self.func.__qualname__}\"")
            traceback.print_exc()
            print('Exception: {}'.format(e))

    def __call__(self, *args, **kwds):
        if not disable_multiprocessing:
            if self.share_arrays:
                args, kwds = self.as_shared(args), self.as_shared(kwds)
            self.parent_conn.send((args, kwds))
            return AsyncResult(self.parent_conn, share_array=self.share_arrays)
        else:
            if self._footprints is not None:
                start_t = time.time()

            result = self.func(*args, **kwds)

            if self._footprints is not None:
                end_t = time.time()
                self._footprints.put((start_t, end_t))
            return SyncResult(result)

    def __del__(self):
        if hasattr(self, 'process'):
            self.process.terminate()

    def footprints(self):
        assert self._footprints is not None, "Footprints are not traced"
        footprints = []
        while not self._footprints.empty():
            footprints.append(self._footprints.get())
        return footprints

class AsyncResult:
    def __init__(self, conn, share_array: bool = False):
        self.conn = conn
        self.share_array = share_array

    def get(self):
        "Block main process, wait for the result and return it"
        # torch.cuda.synchronize()
        result = self.conn.recv()
        if self.share_array:
            result = PersistentProcessWorker.from_shared(result)
        return result

class SyncResult:
    def __init__(self, result):
        self.result = result

    def get(self):
        return self.result

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
def worker(unbounded_method, *, share_arrays: bool = False, trace_footprints: bool = False):
    """
    A decorator that wraps a method as a worker method running in a persistent separate process.
    The process will be created when the method is called for the first time. 
    The wrapped method will return an AsyncResult object, which can be used to get the result.
    
    NOTE: This decorator wrap the method only in the main process. In subprocesses, this decorator does nothing but return the original method.
    """
    if mp.current_process().name != 'MainProcess':
        return unbounded_method
    elif '.' in unbounded_method.__qualname__:
        # For class method
        fname = str(unbounded_method.__name__)
        @wraps(unbounded_method)
        def wrapper2(self, *args, **kwds):
            if not isinstance(getattr(self, fname), PersistentProcessWorker):
                bound_method = unbounded_method.__get__(self)
                setattr(self, fname, PersistentProcessWorker(bound_method, share_arrays=share_arrays, trace_footprints=trace_footprints))
            return getattr(self, fname)(*args, **kwds)
        return wrapper2
    else:
        # For global function
        return PersistentProcessWorker(unbounded_method, trace_footprints=trace_footprints)