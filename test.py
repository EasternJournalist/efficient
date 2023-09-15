import asyncio
from worker_on_child_process import WorkersOnChildProcesses

class XXX(WorkersOnChildProcesses):
    def __init__(self):
        super().__init__(num_workers=4, revive=True)

    def init(self):
        self.x = self.RANK ** 2
    
    def work(self, i):
        print('work', self.RANK)
        return self.x + i


async def main():
    xxx = XXX()
    print(await xxx.run(0))
    print(await xxx.run(1))
    print(await xxx.run(2))
    print(await xxx.run(3))
    print(await xxx.run(4))
    print(await xxx.run(5))
    xxx.terminate()


asyncio.run(main())