from typing import *
from abc import abstractmethod
from queue import Empty, Full
from threading import Thread
from queue import Queue
from multiprocessing import Process
from threading import Thread, Event
import multiprocessing
import threading
import inspect
import time
import uuid
from copy import deepcopy
import itertools

__all__ = [
    'Node', 
    'Link',
    'JobNode',
    'Worker', 
    'WorkerFunction',
    'Provider',
    'ProviderFunction',
    'Pipeline', 
    'Sequential',
    'Parallel',
    'Batch',
    'Unbatch',
]

TERMINATE_CHECK_INTERVAL = 0.5


class _ItemWrapper:
    def __init__(self, data: Union[Any, List[Any]], id: Union[int, List[int]]):
        self.data = data
        self.id = id


class Terminate(Exception):
    pass


def _get_queue_item(queue: Queue, terminate_flag: Event, timeout: float = None) -> _ItemWrapper:
    while True:
        try:
            item: _ItemWrapper = queue.get(block=True, timeout=TERMINATE_CHECK_INTERVAL if timeout is None else min(timeout, TERMINATE_CHECK_INTERVAL))
            if terminate_flag.is_set():
                raise Terminate()
            return item
        except Empty:
            if terminate_flag.is_set():
                raise Terminate()
            
        if timeout is not None:
            timeout -= TERMINATE_CHECK_INTERVAL
            if timeout <= 0:
                raise Empty()


def _put_queue_item(queue: Queue, item: _ItemWrapper, terminate_flag: Event):
    while True:
        try:
            queue.put(item, block=True, timeout=TERMINATE_CHECK_INTERVAL)
            if terminate_flag.is_set():
                raise Terminate()
            return
        except Full:
            if terminate_flag.is_set():
                raise Terminate()

class Node:
    def __init__(self, max_size_in: int = 1, max_size_out: int = 1) -> None:
        self.input: Dict[str, Queue] = {}
        self.output: Dict[str, Queue] = {}
        self.max_size_in = max_size_in
        self.max_size_out = max_size_out

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def terminate(self):
        pass

    @abstractmethod
    def join(self):
        pass
    
    def _get_input_queue(self, key: str = None):
        if key not in self.input:
            self.input[key] = Queue(maxsize=self.max_size_in)
        return self.input[key]
    
    def _get_output_queue(self, key: str = None):
        if key not in self.output:
            self.output[key] = Queue(maxsize=self.max_size_out)
        return self.output[key]
    
    def put(self, data: Any, key: str = None, block: bool = True) -> None:
        item = _ItemWrapper(data, None)
        self._get_input_queue(key).put(item, block=block)
    
    def get(self, key: str = None, block: bool = True) -> Any:
        item: _ItemWrapper = self._get_output_queue(key).get(block=block)
        return item.data

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.terminate()
        self.join()


class JobNode(Node):
    job: Union[Thread, Process]

    def __init__(self, running_as: Literal['thread', 'process'] = 'thread', max_size_in: int = 1, max_size_out: int = 1) -> None:
        super().__init__(max_size_in, max_size_out)
        self.running_as = running_as

    @abstractmethod
    def _job_fn(self, input: Dict[str, Queue], output: Dict[str, Queue], terminate_flag: Event):
        pass

    def start(self):
        if self.running_as == 'thread':
            terminate_flag = threading.Event()
            job = Thread(target=self._job_fn, args=(self.input, self.output, terminate_flag))
        elif self.running_as == 'process':
            terminate_flag = multiprocessing.Event()
            job = Process(target=self._job_fn, args=(self.input, self.output, terminate_flag))
        job.start()
        self.job = job
        self.terminate_flag = terminate_flag

    def terminate(self):
        self.terminate_flag.set()

    def join(self):
        self.job.join()


class Worker(JobNode):
    def __init__(self, running_as: Literal['thread', 'process'] = 'thread', max_size_in: int = 0, max_size_out: int = 0) -> None:
        super().__init__(running_as, max_size_in, max_size_out)

    def init(self) -> None:
        """
        This method is called the the thread is started, to initialize any resources that is only held in the thread.
        """
        pass

    @abstractmethod
    def work(self, *args, **kwargs) -> Union[Any, Dict[str, Any]]:
        """
        This method defines the job that the node should do for each input item. 
        A item obtained from the input queue is passed as arguments to this method, and the result is placed in the output queue.
        The method is executed concurrently with other nodes.
        """
        pass

    def _job_fn(self, input: Dict[str, Queue], output: Dict[str, Queue], terminate_flag: Event):
        self.init()
        try:
            while True:
                args, kwargs = [], {}
                for key, queue in input.items():
                    item = _get_queue_item(queue, terminate_flag)
                    if key is None:
                        args.append(item.data)
                    else:
                        kwargs[key] = item.data
                
                result = self.work(*args, **kwargs)
                
                for key, queue in output.items():
                    if key is None:
                        _put_queue_item(queue, _ItemWrapper(result, item.id), terminate_flag)
                    else:
                        _put_queue_item(queue, _ItemWrapper(result[key], item.id), terminate_flag)
        except Terminate:
            return


class Provider(JobNode):
    """
    A node that provides data to successive nodes. It takes no input and provides data to the output queue.
    """
    def __init__(self, running_as: Literal['thread', 'process'], max_size_out: int = 1) -> None:
        super().__init__(running_as, 0, max_size_out)

    def init(self) -> None:
        """
        This method is called the the thread or process is started, to initialize any resources that is only held in the thread or process.
        """
        pass

    @abstractmethod
    def provide(self) -> Generator[Any, None, None]:
        pass

    def _job_fn(self, input: Dict[str, Queue], output: Dict[str, Queue], terminate_flag: Event):
        self.init()
        try:
            for data in self.provide():
                _put_queue_item(output[None], _ItemWrapper(data, None), terminate_flag)
        except Terminate:
            return


class WorkerFunction(Worker):
    def __init__(self, fn: Callable, running_as: 'thread', max_size_in: int = 1, max_size_out: int = 1) -> None:
        super().__init__(running_as, max_size_in, max_size_out)
        self.fn = fn

    def work(self, *args, **kwargs):
        return self.fn(*args, **kwargs)


class ProviderFunction(Provider):
    def __init__(self, fn: Callable, running_as: 'thread', max_size_out: int = 1) -> None:
        super().__init__(running_as, max_size_out)
        self.fn = fn

    def provide(self):
        for item in self.fn():
            yield item


class Link:
    def __init__(self, src: Queue, dst: Queue):
        self.src = src
        self.dst = dst
        

    def _thread_fn(self, src: Queue, dst: Queue, terminate_flag: Event):
        try:
            while True:
                item = _get_queue_item(src, terminate_flag)
                _put_queue_item(dst, item, terminate_flag)
        except Terminate:
            return
    
    def start(self):
        terminate_flag = threading.Event()
        thread = Thread(target=self._thread_fn, args=(self.src, self.dst, terminate_flag))
        thread.start()
        self.thread = thread
        self.terminate_flag = terminate_flag

    def terminate(self):
        self.terminate_flag.set()

    def join(self):
        self.thread.join()


class Pipeline(Node):
    """
    Pipeline of nodes connected by links.
    """
    def __init__(self, max_size_in: int = 1, max_size_out: int = 1):
        super().__init__(max_size_in, max_size_out)
        self.nodes: List[Node] = []
        self.links: List[Link] = []

    def add(self, node: Node):
        self.nodes.append(node)

    def link(self, src: Union[Node, Tuple[Node, str]], dst: Union[Node, Tuple[Node, str]]):
        """
        Links the output of the source node to the input of the destination node.
        If the source or destination node is None, the pipeline's input or output is used.
        """
        src_node, src_key = src if isinstance(src, tuple) else (src, None)
        if src_node is None:
            src_queue = self._get_input_queue(src_key)
        else:
            src_queue = src_node._get_output_queue(src_key)
        dst_node, dst_key = dst if isinstance(dst, tuple) else (dst, None)
        if dst_node is None:
            dst_queue = self._get_output_queue(dst_key)
        else:
            dst_queue = dst_node._get_input_queue(dst_key)
        self.links.append(Link(src_queue, dst_queue))

    def chain(self, nodes: Iterable[Node]):
        """
        Link the output of each node to the input of the next node.
        """
        nodes = list(nodes)
        for i in range(len(nodes) - 1):
            self.link(nodes[i], nodes[i + 1])

    def start(self):
        for node in self.nodes:
            node.start()
        for link in self.links:
            link.start()

    def terminate(self):
        for node in self.nodes:
            node.terminate()
        for link in self.links:
            link.terminate()

    def join(self):
        for node in self.nodes:
            node.join()
        for link in self.links:
            link.join()

    def __iter__(self):
        providers = [node for node in self.nodes if isinstance(node, Provider)]
        if len(providers) == 0:
            raise ValueError("No provider node found in the pipeline. If you want to iterate over the pipeline, the pipeline must be driven by a provider node.")
        with self:
            # while all(provider.job.is_alive() for provider in providers):
            while True:
                yield self.get()

    def __call__(self, data: Any) -> Any:
        """
        Submit data to the pipeline's input queue, and return the output data asynchronously.
        NOTE: The pipeline must be streamed (i.e., every output item is uniquely associated with an input item) for this to work.
        """
        # TODO


class Sequential(Pipeline):
    """
    Pipeline of nodes in sequential order, where each node takes the output of the previous node as input.
    The order of input and output items is preserved (FIFO)
    """
    def __init__(self, *nodes: Union[Node, Callable], function_running_as: Literal['thread', 'process'] = 'thread', max_size_in: int = 1, max_size_out: int = 1):
        """
        Initialize the pipeline with a list of nodes to execute sequentially.
        ### Parameters:
        - nodes: List of nodes or functions to execute sequentially. Generator functions are wrapped in provider nodes, and other functions are wrapped in worker nodes.
        - function_running_as: Whether to wrap the function as a thread or process worker. Default is 'thread'.
        - max_size_in: Maximum size of the input queue of the pipeline. Default is 0 (unlimited).
        - max_size_out: Maximum size of the output queue of the pipeline. Default is 0 (unlimited).
        """
        super().__init__(max_size_in, max_size_out)
        for node in nodes:
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    node = ProviderFunction(node, function_running_as)
                else:
                    node = WorkerFunction(node, function_running_as)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.add(node)
        self.chain([None, *self.nodes, None])


class Parallel(Pipeline):
    """
    Pipeline of nodes in parallel, where each input item is handed to one of the nodes whoever is available.
    NOTE: The order of the output items is NOT guaranteed to be the same as the input items, depending on how fast the nodes handle their input.
    """
    def __init__(self, *nodes: Union[Node, Callable], num_duplicated_workers: int = None, function_running_as: Literal['thread', 'process'] = 'thread', max_size_in: int = 1, max_size_out: int = 1):
        """
        Initialize the pipeline with a list of nodes to execute in parallel. If a function is given, it is wrapped in a worker node.
        ### Parameters:
        - nodes: List of nodes or functions to execute in parallel. Generator functions are wrapped in provider nodes, and other functions are wrapped in worker nodes.
        - num_duplicated_workers: Number of duplicated workers for each node. If specified, the only one node is allowed and deepcopied for each worker.
        - function_running_as: Whether to wrap the function as a thread or process worker. Default is 'thread'.
        - max_size_in: Maximum size of the input queue of the pipeline. Default is 0 (unlimited).
        - max_size_out: Maximum size of the output queue of the pipeline. Default is 0 (unlimited).
        """
        super().__init__(max_size_in, max_size_out)
        if num_duplicated_workers is not None:
            assert len(nodes) == 1, "Only one node is allowed when `num_duplicated_workers` is specified."
            nodes = [deepcopy(nodes[0]) for _ in range(num_duplicated_workers)]
        for node in nodes:
            if isinstance(node, Node):
                pass
            elif isinstance(node, Callable):
                if inspect.isgeneratorfunction(node):
                    node = ProviderFunction(node, function_running_as)
                else:
                    node = WorkerFunction(node, function_running_as)
            else:
                raise ValueError(f"Invalid node type: {type(node)}")
            self.add(node)
        for i in range(len(nodes)):
            self.chain([None, self.nodes[i], None])


class Batch(JobNode):
    """
    Groups every `batch_size` items into a batch (a list of items) and passes the batch to successive nodes.
    The `patience` parameter specifies the maximum time to wait for a batch to be filled before sending it to the next node,
    i.e., when the earliest item in the batch is out of `patience` seconds, the batch is sent regardless of its size.
    """
    def __init__(self, batch_size: int, patience: float = None, max_size_in: int = 1, max_size_out: int = 1):
        assert batch_size > 0, "Batch size must be greater than 0."
        super().__init__('thread', max_size_in, max_size_out)
        self.batch_size = batch_size
        self.patience = patience

    def _job_fn(self, input: Dict[str, Queue], output: Dict[str, Queue], terminate_flag: Event):
        try:
            while True:
                batch_id, batch_data = [], []
                # Try to fill the batch
                for i in range(self.batch_size):
                    if i == 0 or self.patience is None:
                        timeout = None
                    else:
                        timeout = self.patience - (time.time() - earliest_time)
                        if timeout < 0:
                            break
                    try:
                        item = _get_queue_item(input[None], terminate_flag, timeout)
                    except Empty:
                        break

                    if i == 0:
                        earliest_time = time.time()
                    batch_data.append(item.data)
                    batch_id.append(item.id)

                batch = _ItemWrapper(batch_data, batch_id)
                _put_queue_item(output[None], batch, terminate_flag)
        except Terminate:
            return


class Unbatch(JobNode):
    """
    Ungroups every batch (a list of items) into individual items and passes them to successive nodes.
    """
    def __init__(self, max_size_in: int = 1, max_size_out: int = 1):
        super().__init__('thread', max_size_in, max_size_out)

    def _job_fn(self, input: Dict[str, Queue], output: Dict[str, Queue], terminate_flag: Event):
        try:
            while True:
                batch = _get_queue_item(input[None], terminate_flag)
                for id, data in zip(batch.id or itertools.repeat(None), batch.data):
                    item = _ItemWrapper(data, id)
                    _put_queue_item(output[None], item, terminate_flag)
        except Terminate:
            return
