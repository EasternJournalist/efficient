from efficient import ThreadWorker, Pipeline, SequentialPipeline
import time
from efficient.utils.timeit import timeit

class Square(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x ** 2

class TimesTwo(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x * 2
    
class AddOne(ThreadWorker):
    def work(self, x):
        # time.sleep(0.1)
        return x + 1
    
pipe = SequentialPipeline(
    *[AddOne() for _ in range(1000)]
)
pipe.start()

with timeit("SequentialPipeline"):
    for i in range(1000):
        pipe.put(i)
    # for i in range(100):
    print(pipe.get())
pipe.terminate()

