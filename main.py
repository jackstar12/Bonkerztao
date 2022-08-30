import importlib
import concurrent.futures
import json
from functools import wraps

import click
import time

import asyncio
import logging
import aioredis

redis = aioredis.from_url('redis://localhost')
pubsub = redis.pubsub()

STOCKS = ['eth']


STATUS = "status"


INDEX = "index"
STOP = "stop"
START = "start"


def join_args(*args):
    return ':'.join([str(arg) for arg in args if arg])


def _wrap(self, callback):
    @wraps(callback)
    def wrapper(event: dict, *args, **kwargs):
        self._logger.info(f'Redis Event: {event=} {args=} {kwargs=}')
        data = json.loads(event['data'].decode('utf-8'))
        callback(data)

    return wrapper

async def listen():
    async for _ in pubsub.listen():
        pass


def wrap(callback):
    @wraps(callback)
    def wrapper(event: dict, *args, **kwargs):
        print(f'Redis Event: {event=} {args=} {kwargs=}')
        data = json.loads(event['data'].decode('utf-8'))
        callback(data)
    return wrapper


async def sub_channel(channel, callback=None):
    subscription = {channel: wrap(callback)}
    print(f'Sub: {subscription}')
    await pubsub.subscribe(**subscription)


def open_order_update_main(stock: str):
    stock_config = importlib.import_module(f'bots.{stock}.config')

    print('moin moin from ', stock)
    time.sleep(stock_config.SLEEP_TIME)
    return True


async def watch(executor, stock: str):
    while True:
        print(f'Starting stock {stock}')

        await redis.set(join_args(STATUS, stock), 1)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, open_order_update_main, stock)

        print(f'Stock {stock} finished, restarting, Result: {result}')


async def execute():
    result = await redis.ping()
    if not result:
        print('REDIS CONNECTION NOT WORKING!')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        all_tasks = []

        # Main (manager von spezifischen prozessen)
        # Subscribe = Empfangen

        def on_start(data: dict):
            symbol = data['symbol']
            try:
                # gleich zu import bots.btc.config
                stock_config = importlib.import_module(f'bots.{symbol}.config')
            except ImportError:
                logging.error(f'Stock does not exist: {symbol}')
                return

            # Optional Feature: Custom Main Import
            # try:
            #     stock_main = importlib.import_module(f'bots.{stock}.main')
            # except ImportError:
            #     pass  # Nix custom

            all_tasks.append(
                asyncio.create_task(
                    watch(executor, symbol)
                )
            )

        await sub_channel(START, callback=on_start)

        # Höre auf redis nachrichten
        asyncio.create_task(
            listen()
        )

        for stock in STOCKS:
            try:
                # gleich zu import bots.btc.config
                stock_config = importlib.import_module(f'bots.{stock}.config')
            except ImportError:
                logging.error(f'Stock does not exist: {stock}')
                return

            # Optional Feature: Custom Main Import
            # try:
            #     stock_main = importlib.import_module(f'bots.{stock}.main')
            # except ImportError:
            #     pass  # Nix custom

            all_tasks.append(
                asyncio.create_task(
                    watch(executor, stock)
                )
            )
            await asyncio.sleep(1)
        await asyncio.gather(*all_tasks)


@click.command()
def main():
    """
    Einstiegspunkt für unser Skript
    """

    asyncio.run(execute())


if __name__ == "__main__":
    main()
