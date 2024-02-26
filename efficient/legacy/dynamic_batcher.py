from typing import *
import asyncio


__all__ = ["DynamicBatcher"]


class Batch:
    def __init__(self, fn: Callable[[Any], Coroutine], max_batch_size: int):
        self.fn = fn
        self.max_batch_size = max_batch_size
        self.inputs: List[str] = []
        self.outputs: List[str] = None
        self.lock = asyncio.Lock()
        self.is_closed = False
        self.is_launched = asyncio.Event()
        self.outputs_ready = asyncio.Event()

    async def add(self, input) -> int:
        async with self.lock:
            if self.is_closed is True:
                return None
            idx = len(self.inputs)
            self.inputs.append(input)
            if len(self.inputs) == self.max_batch_size:
                self.is_closed = True
            return idx

    async def launch(self, patience: float):
        if not self.is_closed:
            try:
                await asyncio.wait_for(self.is_launched.wait(), timeout=patience)
            except asyncio.exceptions.TimeoutError:
                pass
        async with self.lock:
            self.is_closed = True
            if self.is_launched.is_set():
                return
            self.is_launched.set()
        self.outputs = await self.fn(self.inputs)
        self.outputs_ready.set()

    async def get_outputs(self, idx: int) -> str:
        await self.outputs_ready.wait()
        return self.outputs[idx]


class DynamicBatcher:
    """
    A dynamic batcher that accepts inputs one by one asynchronously and launch them as a batch when the batch is full or the patience is reached.
    """
    def __init__(self, batch_fn: Callable[[List[Any]], Awaitable[List[Any]]], max_batch_size: int):
        """
        Args:
            batch_fn: A coroutine function that accepts a list (Batch) of inputs and returns a list of outputs.
            max_batch_size: The maximum size of a batch.
        """
        self.fn = batch_fn
        self.max_batch_size = max_batch_size
        self.current_batch: Batch = Batch(self.fn, self.max_batch_size)
        self.lock = asyncio.Lock()
    
    async def run(self, input: Any, patience: float) -> Any:
        async with self.lock:
            while True:
                batch = self.current_batch
                idx = await batch.add(input)
                if idx is not None:
                    break
                self.current_batch = Batch(self.fn, self.max_batch_size)

        await batch.launch(patience=patience)

        return await batch.get_outputs(idx)