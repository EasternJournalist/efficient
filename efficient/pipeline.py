from typing import *
import threading
from abc import abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
import inspect


__all__ = ['Worker', 'ThreadWorker', 'Pipeline', 'SequentialPipeline']

class Worker:
    def __init__(self) -> None:
        self.inputs: Dict[str, 'Link'] = {}
        self.outputs: Dict[str, 'Link'] = {}

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    def __call__(self, arg: Any):
        # TODO: ensure FIFO behavior
        self.inputs[None].put(arg)
        return self.outputs[None].get(block=True)

    @abstractmethod
    def empty(self) -> bool:
        return 
    
    def put(self, item: Any, key: str = None, block: bool = True):
        self.inputs[key].put(item, block=block)
    
    def get(self, key: str = None, block: bool = True):
        return self.outputs[key].get(block=block)
    

class Link:
    def __init__(self):
        self.queue = Queue()
        self.src = []
        self.dst = []
    
    def add_src(self, src: Union[Worker, Tuple[Worker, str]]):
        if isinstance(src, Worker):
            src_worker, src_key = src, None
        else:
            src_worker, src_key = src
        self.src.append((src_worker, src_key))

    def add_dst(self, dst: Union[Worker, Tuple[Worker, str]]):
        if isinstance(dst, Worker):
            dst_worker, dst_key = dst, None
        else:
            dst_worker, dst_key = dst
        self.dst.append((dst_worker, dst_key))

    def merge(self, other: 'Link'):
        """
        Merge with another link.
        """
        if other is not None:
            self.src.extend(other.src)
            self.dst.extend(other.dst)
        return self

    def get(self, block: bool = True):
        return self.queue.get(block=block)
    
    def put(self, item: Any, block: bool = True):
        self.queue.put(item, block=block)


class ThreadWorker(Worker):
    def __init__(self) -> None:
        super().__init__()
        self._size = 0
        self._lock = threading.Lock()

    def init_in_thread(self) -> None:
        """
        This method is called the the worker thread is started, to initialize any resources that is only held in the thread.
        """
        pass

    @abstractmethod
    def work(self, *args, **kwargs):
        """
        This method defines the job that the worker should do. 
        Data obtained from the input queue is passed as arguments to this method, and the result is placed in the output queue.
        The method is executed concurrently with other workers.
        """
        pass

    def _thread_fn(self):
        self.init_in_thread()
        while True:
            args, kwargs = [], {}
            
            for key, link in self.inputs.items():
                if key is None:
                    args = link.get(block=True)
                else:
                    kwargs[key] = link.get(block=True)
            
            results = self.work(*args, **kwargs)
            
            for key, link in self.outputs.items():
                if key is None:
                    link.put(results, block=True)
                else:
                    link.put(results[key], block=True)
            
            with self._lock:
                self._size -= 1

    def put(self, item: Any, key: str = None, block: bool = True):
        with self._lock:
            self._size += 1
        super().put(item, key, block)
    
    def empty(self) -> bool:
        return self._size == 0

    def start(self):
        self.thread = threading.Thread(target=self._thread_fn)
        self.thread.start()

    def terminate(self):
        # TODO: implement terminate logic
        raise NotImplementedError()


class Pipeline(Worker):
    def __init__(self):
        super().__init__()
        self.workers: List[Worker] = []

    def add(self, worker: Worker):
        self.workers.append(worker)

    def link(self, src: Union[Worker, Tuple[Worker, str]] = None, dst: Union[Worker, Tuple[Worker, str]] = None):
        """
        Links the output of the source worker to the input of the destination worker.
        If the source or destination worker is None, the pipeline's input or output is used.
        If the source or destination key is None, assume there is only a single input or output queue.
        """
        src_worker, src_key = src if isinstance(src, tuple) else (src, None)
        dst_worker, dst_key = dst if isinstance(dst, tuple) else (dst, None)

        src_links = self.inputs if src_worker is None else src_worker.outputs
        dst_links = self.outputs if dst_worker is None else dst_worker.inputs
        
        link = Link()
        link.merge(src_links.get(src_key))
        link.merge(dst_links.get(dst_key))
        link.add_dst((src_worker, src_key))
        link.add_src((dst_worker, dst_key))
        src_links[src_key] = dst_links[dst_key] = link

    def chain(self, workers: Iterable[Worker]):
        workers = list(workers)
        for i in range(len(workers) - 1):
            self.link(workers[i], None, workers[i + 1], None)

    def start(self):
        for worker in self.workers:
            worker.start()

    def terminate(self):
        for worker in self.workers:
            worker.terminate()

    def empty(self) -> bool:
        return all(worker.empty() for worker in self.workers)


class SequentialPipeline(Pipeline):
    def __init__(self, *workers: Worker):
        super().__init__()
        for worker in workers:
            self.add(worker)
        self.chain([None] + list(workers) + [None])

 