from asyncio import gather






class Tasksmanager():

    def __init__(self) -> None:
        self.tasks = []

    async def background_task(self, task_queue):
        coro = []
        await gather(*coro)

    async def create_alert(self):
        pass