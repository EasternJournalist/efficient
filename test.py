from efficient import ThreadWorker, Pipeline, SequentialPipeline
import time

class Square(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x ** 2

class TimesTwo(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x ** 2
    
class AddOne(ThreadWorker):
    def work(self, x):
        time.sleep(1.0)
        return x + 1
    
pipe = SequentialPipeline(
    Square(),
    TimesTwo(),
    AddOne()
)

for i in range(10):
    pipe.put(i)

while not pipe.empty():
    print(pipe.get())