import asyncio
import sys
import aioconsole

class aRobot:

    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop
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

class bRobot:
        
    def __init__(self) -> None:
        self.loop = None
        self.coros : dict = {} # A dictionary to store the coroutines and their name
        self.isrunning : bool = True # A flag to indicate if robot is running
        self.tasks : dict = {} # A dictionary to store the coroutines and their name

    def add_task(self, name: str, coro: asyncio.coroutines) -> None:
        # add a task to the dictionary with a name  and a coroutine
        self.coros[name] = coro
    
    async def run_task(self, name : str) -> None:
        # run a task by its name
        if name in self.coros:
            self.tasks[name] = self.loop.create_task(self.coros[name])
            # await task
        else:
            print(f'No such task {name}')
    
    async def stop_task(self, name: str) -> None:
        # stop a task by its name
        if name in self.tasks:
            task = self.tasks[name]
            if not task.done():
                task.cancel()
                await task
        else:
            print(f'No such task {name}')

    async def stop_all_tasks(self) -> None:
        # stop all the tasks
        for name, task in self.tasks.items():
            if not task.done():
                task.cancel()
                await task
    
    async def show_menu(self) ->None :
        # Show a text menu with the options to the user
        print('Welcome to the robot interface. Please chose an option')
        print('1. Say Hello')
        print('2. Count Numbers')
        print('3. Add two numbers')
        print('4. Stop counting')
        print('5. Stop all tasks')
        print('6. Check running tasks')
        print('7. Check tasks return')
        print('8. Exit')

    async def get_input(self) -> str:
        # get the user input and return it as a string
        return await aioconsole.ainput('==>')

    async def check_tasks(self):
        tasks = asyncio.all_tasks()
        for task in tasks:
            print(f'> {task.get_name()}, {task.get_coro()}')

    async def process_input(self, user_input: str):
        # Process the user input and execute the corresponding task
        if user_input == '1':
            self.add_task("say_hello", self.say_hello())
            await self.run_task('say_hello')
        elif user_input == '2':
            self.add_task('count', self.count())
            await self.run_task('count')
        elif user_input == '3':
            a = int(await aioconsole.ainput('Enter the first number: '))
            b = int(await aioconsole.ainput('Enter the second number: '))
            self.add_task('add', self.add(a, b))
            await self.run_task('add')
        elif user_input == '4':
            await self.stop_task('count')
        elif user_input == '5':
            await self.stop_all_tasks()
        elif user_input == '6':
            await self.check_tasks()
        elif user_input == '7':
            pass
        elif user_input == '8':
            self.isrunning = False
        else:
            print('Invalid option')
        
    async def start(self):
        #run the robot
        while self.isrunning:
            await self.show_menu()
            self.loop = asyncio.get_running_loop()
            user_input = await self.get_input()
            await self.process_input(user_input)

    async def say_hello(self):
        await asyncio.sleep(5)
        return 'Hello, I am a robot.'

    async def count(self):
        i = 0
        while True:
            print(i)
            i += 1
            await asyncio.sleep(5)

    async def add(self, a, b):
        await asyncio.sleep(5)
        print(f'{a} + {b} = {a + b}')
        




if __name__ == "__main__":

    robot = bRobot()
    asyncio.run(robot.start())