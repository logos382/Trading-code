import asyncio

class aRobot():

    def __init__(self) -> None:
        self.isrunning = True

    async def start(self) -> None:
        while self.isrunning:
            with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(self.menu())
                task2 = tg.create_task(self.get_input())      

    async def menu(self) -> None:
        print("1. Add task1")
        print("2. Add task2")
        print("3. Check all tasks")
        print("4. ")
        print("5. Exit")

    async def get_input(self):
        usr_input = input('==>')
        return usr_input






if __name__ == "__main__":

    asyncio.run(aRobot.start())