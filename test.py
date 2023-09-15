import asyncio
from worker_on_child_process import WorkersOnChildProcesses


class XXX(WorkersOnChildProcesses):
    def __init__(self):
        super().__init__(num_workers=1, revive=True)

    def init(self):
        self.x = self.RANK ** 2
    
    def work(self, i):
        raise RuntimeError
        print('work', self.RANK)
        return self.x + i


async def main():
    xxx = XXX()
    for i in range(10):
        try:
            print(await xxx.run(i))
        except:
            pass
    xxx.terminate()


asyncio.run(main())