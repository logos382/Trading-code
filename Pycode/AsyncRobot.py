import asyncio

class aRobot:

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.isrunning = True
        self.tasks = {}

    async def asleep(self) -> None:
        await asyncio.sleep(20)
        print("asleep completed")
        

    async def start(self) -> None:
        while self.isrunning:
            async with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(self.menu())
                task2 = tg.create_task(self.get_input())


    async def menu(self) -> None:
        print("1. Add task1")
        print("3. Check all tasks")
        print("5. Exit")

    async def get_input(self):
        usr_input = input('==> ')
        await self.select_task(int(usr_input))
    
    async def select_task(self,input: int):
        if input == 1:
            self.schedule_task(self.asleep)
        elif input == 3:
            await self.check_tasks()
        elif input == 5:
            self.isrunning = False
        else:
            pass

    async def schedule_task(self, coro_func):
        task = self.loop.create_task(coro_func)
 
    async def check_tasks(self):
        tasks = asyncio.all_tasks()
        for task in tasks:
            print(f'> {task.get_name()}, {task.get_coro()}')


if __name__ == "__main__":

    robot = aRobot()
    asyncio.run(robot.start())