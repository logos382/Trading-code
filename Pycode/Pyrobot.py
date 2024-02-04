import json
import time as time_true
import pathlib
import pandas as pd
import aioconsole
import asyncio

from datetime import datetime
from datetime import timezone
from datetime import timedelta

from typing import List
from typing import Dict
from typing import Union

from sqlalchemy.ext.asyncio import create_async_engine

import ccxt.async_support as ccxt


class Robot():

    def __init__(self) -> None:
        # A common queue holding the jobs for the workers.
        self.target_q = asyncio.Queue(10) #10 is me max number of jobs that can stay in the queue 0 = infinite jobs
        # The list of workers
        self.workers = None

    @staticmethod
    def create_asyncengine(sql_uri: str, engine_pool_size: int = 5, engine_max_overflow: int = 10) -> create_async_engine:
        try:
            # create the engine to write/read into the sql database(e.g. an sqlite db)
            asyncengine = create_async_engine(
                sql_uri,
                pool_size = engine_pool_size,  # Adjust the pool size based on your needs
                max_overflow = engine_max_overflow  # Adjust the max_overflow based on your needs
                )
        except TypeError:
            # The pool_size argument won't work for the default SQLite setup in SQLAlchemy, try without
            asyncengine = create_async_engine(sql_uri)

        return asyncengine
    
    @staticmethod    
    def create_exchanges(exchange_id: str, type: str = 'future')-> ccxt.exchanges:
        exchange = getattr(ccxt, exchange_id)({
            'options': {'defaultType': type ,  # 'spot', 'future', 'margin', 'delivery'
                        },})
        exchange.enableRateLimit = True

        return exchange


    async def user_input():
        print("Choose an action:")
        print("1. Perform Action 1")
        print("2. Perform Action 2")
    
        user_choice = await aioconsole.ainput("Enter your choice: ")
    
    

