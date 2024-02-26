import time
from typing import *
import numpy as np
import random

from efficient import *

class Square(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x ** 2

class BatchTimesTwo(ThreadWorker):
    def __init__(self, rank: int = 0) -> None:
        super().__init__()
        self.rank = rank

    def work(self, batch: List[float]):
        time.sleep(random.random())
        # print(f"Rank: {self.rank}, BatchSize: {len(batch)}\n", end="")
        batch = np.array(batch, dtype=float) * 2
        batch = batch.tolist()
        return batch
    
class TimesTwo(ThreadWorker):
    def work(self, x):
        return x * 2

class AddOne(ThreadWorker):
    def work(self, x):
        time.sleep(0.1)
        return x + 1

if __name__ == '__main__':
    pipe = Sequential(
        AddOne(),
        Batch(batch_size=4, patience=0.2),
        Parallel(
            BatchTimesTwo(0),
            BatchTimesTwo(1),
            BatchTimesTwo(2)
        ),
        Unbatch(),
    )
    pipe.start()

    # 1. Put & Get. The order of getting results is not guaranteed.
    with timeit("SequentialPipeline"):
        for i in range(15):
            pipe.put(i)
        for i in range(15):
            pipe.get()
    
    # # 2. Call in function manner, the input and output are paired.
    # # Return futures.
    # with timeit("SequentialPipeline"):
    #     for i in range(15):
    #         pipe(i)

    pipe.terminate()
    pipe.join()
