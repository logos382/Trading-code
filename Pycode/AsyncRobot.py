from typing import Dict, Coroutine, Optional
from asyncio import AbstractEventLoop, Task

import asyncio
import sys
import aioconsole
from Dataoperator import DataFetcher, DataReader, DataWriter


class aRobot:
        
    def __init__(self,
                 data_fetcher : DataFetcher,
                 data_reader : DataReader, 
                 data_writer : DataWriter) -> None:
        self.loop : AbstractEventLoop = None
        self.coros : Dict [str, Coroutine] = {} # A dictionary to store the coroutines and their name
        self.tasks : Dict [str, Task] = {} # A dictionary to store the running tasks and their name
        self.is_running : bool = True # A flag to indicate if robot is running
        self.data_fetcher : DataFetcher = data_fetcher
        self.data_reader : DataReader = data_reader
        self.data_writer : DataWriter = data_writer

    def add_coro(self, name: str, coro: Coroutine) -> None:
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
                try:
                    task.cancel()
                    await task
                except asyncio.exceptions.CancelledError:
                    print(f'Task {task.get_name()} cancelled')
        else:
            print(f'No such task {name}')

    async def stop_all_tasks(self) -> None:
        # stop all the tasks
        for name, task in self.tasks.items():
            if not task.done():
                try:
                    task.cancel()
                    await task                
                except asyncio.exceptions.CancelledError:
                    print(f'Task {task.get_name()} cancelled')
    
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

    async def check_tasks(self) -> None:
        tasks = asyncio.all_tasks()
        for task in tasks:
            print(f'> {task.get_name()}, {task.get_coro()}')
    
    async def check_returns(self)-> None:
        tasks = asyncio.all_tasks()
        print(f'> {len(tasks)} tasks returned')
        for task in tasks:
            if task.done():
                print(f'> {task.get_name()}, {task.get_coro()} returned {task.result()}')

    async def process_input(self, user_input: str) -> None:
        # Process the user input and execute the corresponding task
        if user_input == '1':
            self.add_coro("say_hello", self.say_hello())
            await self.run_task('say_hello')
        elif user_input == '2':
            self.add_coro('count', self.count())
            await self.run_task('count')
        elif user_input == '3':
            a = int(await aioconsole.ainput('Enter the first number: '))
            b = int(await aioconsole.ainput('Enter the second number: '))
            self.add_coro('add', self.add(a, b))
            await self.run_task('add')
        elif user_input == '4':
            await self.stop_task('count')
        elif user_input == '5':
            await self.stop_all_tasks()
        elif user_input == '6':
            await self.check_tasks()
        elif user_input == '7':
            await self.check_returns()
        elif user_input == '8':
            self.is_running = False
        else:
            print('Invalid option')
        
    async def start(self) -> None:
        #run the robot
        while self.is_running:
            self.loop = asyncio.get_running_loop()
            await self.show_menu()
            user_input = await self.get_input()
            await self.process_input(user_input)

    async def say_hello(self) -> str:
        await asyncio.sleep(5)
        return 'Hello, I am a robot.'

    async def count(self) -> None:
        i: int = 0
        while i < 10:
            print(i)
            i += 1
            await asyncio.sleep(5)

    async def add(self, a: int, b: int) -> None:
        await asyncio.sleep(5)
        print(f'{a} + {b} = {a + b}')
        




if __name__ == "__main__":

    robot : aRobot = aRobot(data_fetcher=DataFetcher, data_reader=DataReader, data_writer=DataWriter )
    asyncio.run(robot.start(), debug=True)