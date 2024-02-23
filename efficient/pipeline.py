from typing import *
import threading
from abc import abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
import inspect


__all__ = ['Worker', 'ThreadWorker', 'Pipeline', 'SequentialPipeline']


class _TerminateSignal:
    pass


class Worker:
    def __init__(self) -> None:
        self.input: Dict[str, Queue] = {}
        self.output: Dict[str, Queue] = {}

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    def __call__(self, arg: Any):
        # TODO: ensure FIFO behavior
        self.input[None].put(arg)
        return self.output[None].get(block=True)

    @abstractmethod
    def empty(self) -> bool:
        return 
    
    def _get_input_queue(self, key: str = None):
        if key not in self.input:
            self.input[key] = Queue()
        return self.input[key]
    
    def _get_output_queue(self, key: str = None):
        if key not in self.output:
            self.output[key] = Queue()
        return self.output[key]
    
    def put(self, item: Any, key: str = None, block: bool = True):
        self.input[key].put(item, block=block)
    
    def get(self, key: str = None, block: bool = True):
        return self.output[key].get(block=block)


class Link:
    def __init__(self, src: Queue, dst: Queue):
        self.src = src
        self.dst = dst

    def _thread_fn(self):
        while True:
            item = self.src.get(block=True)
            if isinstance(item, _TerminateSignal):
                break
            self.dst.put(item, block=True)
    
    def start(self):
        self.thread = threading.Thread(target=self._thread_fn)
        self.thread.start()

    def terminate(self):
        self.src.put(_TerminateSignal(), block=False)
        self.thread.join()


class ThreadWorker(Worker):
    def __init__(self) -> None:
        super().__init__()

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
        _terminate_flag = False
        while True:
            args, kwargs = [], {}
            for key, queue in self.input.items():
                item = queue.get(block=True)
                if isinstance(item, _TerminateSignal):
                    _terminate_flag = True
                    break
                if key is None:
                    args.append(item)
                else:
                    kwargs[key] = item
            if _terminate_flag:
                break
            
            results = self.work(*args, **kwargs)
            
            for key, queue in self.output.items():
                if key is None:
                    queue.put(results, block=True)
                else:
                    queue.put(results[key], block=True)

    def start(self):
        self.thread = threading.Thread(target=self._thread_fn)
        self.thread.start()

    def terminate(self):
        for key, queue in self.input.items():
            queue.put(_TerminateSignal(), block=True)
        self.thread.join()


class Pipeline(Worker):
    def __init__(self):
        super().__init__()
        self.workers: List[Worker] = []
        self.links: List[Link] = []

    def add(self, worker: Worker):
        self.workers.append(worker)

    def link(self, src: Union[Worker, Tuple[Worker, str]], dst: Union[Worker, Tuple[Worker, str]]):
        """
        Links the output of the source worker to the input of the destination worker.
        If the source or destination worker is None, the pipeline's input or output is used.
        """
        src_worker, src_key = src if isinstance(src, tuple) else (src, None)
        if src_worker is None:
            src_queue = self._get_input_queue(src_key)
        else:
            src_queue = src_worker._get_output_queue(src_key)
        dst_worker, dst_key = dst if isinstance(dst, tuple) else (dst, None)
        if dst_worker is None:
            dst_queue = self._get_output_queue(dst_key)
        else:
            dst_queue = dst_worker._get_input_queue(dst_key)
        self.links.append(Link(src_queue, dst_queue))

    def chain(self, workers: Iterable[Worker]):
        workers = list(workers)
        for i in range(len(workers) - 1):
            self.link(workers[i], workers[i + 1])

    def start(self):
        for worker in self.workers:
            worker.start()
        for link in self.links:
            link.start()

    def terminate(self):
        for worker in self.workers:
            worker.terminate()
        for link in self.links:
            link.terminate()


class SequentialPipeline(Pipeline):
    def __init__(self, *workers: Worker):
        super().__init__()
        for worker in workers:
            self.add(worker)
        self.chain([None] + list(workers) + [None])

 