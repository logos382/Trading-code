import json
import time as time_true
import pathlib
import pandas as pd

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
        pass

    def create_asyncengine(sql_uri: str, engine_pool_size: int = 5, engine_max_overflow: int = 10) -> create_async_engine:
        """_summary_

        Args:
            sql_uri (str): _description_
            pool_size (int, optional): _description_. Defaults to 5.
            max_overflow (int, optional): _description_. Defaults to 10.

        Returns:
            create_async_engine: _description_
        """
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
    
    def create_exchanges(exchange_id: str, type: str = 'future')-> ccxt.exchanges:
        """_summary_

        Args:
            exchange_id (str): _description_
            type (str, optional): _description_. Defaults to 'future'.

        Returns:
            ccxt.exchanges: _description_
        """
        
        exchange = getattr(ccxt, exchange_id)({
            'options': {'defaultType': type ,  # 'spot', 'future', 'margin', 'delivery'
                        },})
        exchange.enableRateLimit = True

        return exchange


