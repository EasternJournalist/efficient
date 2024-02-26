from typing import *
import threading
from abc import abstractmethod
from queue import Empty
from multiprocessing import Queue, Process
import inspect
import time
import uuid

__all__ = [
    'Node', 
    'Link',
    'ThreadNode',
    'ThreadWorker', 
    'ProcessWorker',
    'Pipeline', 
    'Sequential',
    'Parallel',
    'Batch',
    'Unbatch',
]


class _ItemWrapper:
    def __init__(self, data: Union[Any, List[Any]], id: Union[int, List[int]]):
        self.data = data
        self.id = id

class _TerminateSignal(_ItemWrapper):
    def __init__(self):
        super().__init__(None, None)


class Node:
    def __init__(self) -> None:
        self.input: Dict[str, Queue] = {}
        self.output: Dict[str, Queue] = {}

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
            self.input[key] = Queue()
        return self.input[key]
    
    def _get_output_queue(self, key: str = None):
        if key not in self.output:
            self.output[key] = Queue()
        return self.output[key]
    
    def put(self, data: Any, key: str = None, block: bool = True) -> None:
        item = _ItemWrapper(data, None)
        self._get_input_queue(key).put(item, block=block)
    
    def get(self, key: str = None, block: bool = True) -> Any:
        item: _ItemWrapper = self._get_output_queue(key).get(block=block)
        return item.data


class ThreadNode(Node):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def thread_fn(self):
        pass

    def start(self):
        thread = threading.Thread(target=self.thread_fn)
        thread.start()
        self.thread = thread

    def terminate(self):
        for key, queue in self.input.items():
            queue.put(_TerminateSignal(), block=False)

    def join(self):
        self.thread.join()


class ThreadWorker(ThreadNode):
    def __init__(self) -> None:
        super().__init__()

    def init_in_thread(self) -> None:
        """
        This method is called the the thread is started, to initialize any resources that is only held in the thread.
        """
        pass

    @abstractmethod
    def work(self, *args, **kwargs):
        """
        This method defines the job that the node should do. 
        Data obtained from the input queue is passed as arguments to this method, and the result is placed in the output queue.
        The method is executed concurrently with other nodes.
        """
        pass

    def thread_fn(self):
        self.init_in_thread()
        _terminate_flag = False
        while True:
            args, kwargs = [], {}
            for key, queue in self.input.items():
                item: _ItemWrapper = queue.get(block=True)
                if isinstance(item, _TerminateSignal):
                    _terminate_flag = True
                    break
                if key is None:
                    args.append(item.data)
                else:
                    kwargs[key] = item.data
            if _terminate_flag:
                break
            
            result = self.work(*args, **kwargs)
            
            for key, queue in self.output.items():
                if key is None:
                    queue.put(_ItemWrapper(result, item.id), block=True)
                else:
                    queue.put(_ItemWrapper(result[key], item.id), block=True)


class ProcessNode(Node):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def process_fn(self):
        pass
    
    def start(self):
        process = Process(target=self.process_fn)
        process.start()
        self.process = process
    
    def terminate(self):
        for key, queue in self.input.items():
            queue.put(_TerminateSignal(), block=False)
    
    def join(self):
        self.process.join()


class ProcessWorker(ProcessNode):
    def __init__(self) -> None:
        super().__init__()

    def init_in_process(self) -> None:
        """
        This method is called the the node process is started, to initialize any resources that is only held in the process.
        """
        pass

    @abstractmethod
    def work(self, *args, **kwargs):
        """
        This method defines the job that the node should do. 
        Data obtained from the input queue is passed as arguments to this method, and the result is placed in the output queue.
        The method is executed concurrently with other nodes.
        """
        pass

    def process_fn(self):
        self.init_in_process()
        _terminate_flag = False
        while True:
            args, kwargs = [], {}
            for key, queue in self.input.items():
                item: _ItemWrapper = queue.get(block=True)
                if isinstance(item, _TerminateSignal):
                    _terminate_flag = True
                    break
                if key is None:
                    args.append(item.data)
                else:
                    kwargs[key] = item.data
            if _terminate_flag:
                break
            
            result = self.work(*args, **kwargs)
            
            for key, queue in self.output.items():
                if key is None:
                    queue.put(_ItemWrapper(result, item.id), block=True)
                else:
                    queue.put(_ItemWrapper(result[key], item.id), block=True)


class Link:
    def __init__(self, src: Queue, dst: Queue):
        self.src = src
        self.dst = dst

    def thread_fn(self):
        while True:
            item: _ItemWrapper = self.src.get(block=True)
            if isinstance(item, _TerminateSignal):
                break
            self.dst.put(item, block=True)
    
    def start(self):
        self.thread = threading.Thread(target=self.thread_fn)
        self.thread.start()

    def terminate(self):
        self.src.put(_TerminateSignal(), block=True)

    def join(self):
        self.thread.join()


class Pipeline(Node):
    "Pipeline of nodes connected by links."
    def __init__(self):
        super().__init__()
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
    def __init__(self, *nodes: Node):
        super().__init__()
        for node in nodes:
            self.add(node)
        self.chain([None, *nodes, None])


class Parallel(Pipeline):
    """
    Pipeline of nodes in parallel, where each input item is handed to one of the nodes whoever is available.
    NOTE: The order of the output items is NOT guaranteed to be the same as the input items, depending on how fast the nodes handle their input.
    """
    def __init__(self, *nodes: Node):
        super().__init__()
        for node in nodes:
            self.add(node)
        for i in range(len(nodes)):
            self.chain([None, nodes[i], None])

 
class Batch(ThreadNode):
    """
    Groups every `batch_size` items into a batch (a list of items) and passes the batch to successive nodes.
    The `patience` parameter specifies the maximum time to wait for a batch to be filled before sending it to the next node,
    i.e., when the earliest item in the batch is out of `patience` seconds, the batch is sent regardless of its size.
    """
    def __init__(self, batch_size: int, patience: float = None):
        super().__init__()
        self.batch_size = batch_size
        self.patience = patience

    def thread_fn(self):
        _terminate_flag = False
        while True:
            batch_id, batch_data = [], []
            
            for i in range(self.batch_size):
                if i == 0 or self.patience is None:
                    timeout = None
                else:
                    timeout = self.patience - (time.time() - earliest_time)
                    if timeout < 0:
                        break
                try:
                    item: _ItemWrapper = self.input[None].get(block=True, timeout=timeout)
                except Empty:
                    break # No more items available within the patience time
                if i == 0:
                    earliest_time = time.time()
                if isinstance(item, _TerminateSignal):
                    _terminate_flag = True
                    break
                batch_data.append(item.data)
                batch_id.append(item.id)

            if _terminate_flag:
                break
            batch = _ItemWrapper(batch_data, batch_id)
            self.output[None].put(batch, block=False)


class Unbatch(ThreadNode):
    """
    Ungroups every batch (a list of items) into individual items and passes them to successive nodes.
    """
    def __init__(self):
        super().__init__()

    def thread_fn(self):
        while True:
            batch: _ItemWrapper = self.input[None].get(block=True)
            if isinstance(batch, _TerminateSignal):
                break
            for id, data in zip(batch.id, batch.data):
                item = _ItemWrapper(data, id)
                self.output[None].put(item, block=True)
