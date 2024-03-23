# sqlalchemy 2.0.21
# pandas 2.1.1

from typing import Dict, Coroutine, Optional
from sqlalchemy.ext.asyncio import AsyncEngine

import asyncio
import logging
from asyncio import gather, run
from sqlalchemy.ext.asyncio import create_async_engine
import time
import ccxt.async_support as ccxt
from Allertmanager import AlertManager
from Dataoperator import DataFetcher, DataReader, DataWriter
from AsyncRobot import aRobot




def create_asyncengine(sql_uri: str, engine_pool_size: Optional[int] = 5, engine_max_overflow: Optional[int] = 10) -> AsyncEngine:
    try:
        # create the engine to write/read into the sql database(e.g. an sqlite db)
        async_engine = create_async_engine(
            sql_uri,
            engine_pool_size = engine_pool_size,  # Adjust the pool size based on your needs
            engine_max_overflow = engine_max_overflow  # Adjust the max_overflow based on your needs
            )
    except TypeError:
        # The pool_size argument won't work for the default SQLite setup in SQLAlchemy, try without
        async_engine = create_async_engine(sql_uri,
                                           engine_max_overflow = engine_max_overflow  # Adjust the max_overflow based on your needs
                                           )
    return async_engine


if __name__ == "__main__":

    async_engine = create_asyncengine(
        'sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db'
    )
    data_fetcher : DataFetcher = DataFetcher()
    data_reader : DataReader = DataReader(async_engine)
    data_writer : DataWriter = DataWriter(async_engine)
    robot : aRobot = aRobot()
    asyncio.run(robot.start())