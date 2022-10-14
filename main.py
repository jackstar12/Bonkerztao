import importlib
import concurrent.futures
import json

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

START_ALL = "start_all"
STOP_ALL = "stop_all"


def join_args(*args):
    return ':'.join(args)


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


async def get_index_price(symbol: str):
    # Hol index preis für das bestimmte symbol
    pass


async def execute():
    result = await redis.ping()
    if not result:
        print('REDIS CONNECTION NOT WORKING!')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        all_symbols = []

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

            all_symbols.append(symbol)
            asyncio.create_task(
                watch(executor, symbol)
            )

        await pubsub.subscribe(START)
        await pubsub.subscribe(START_ALL)
        await pubsub.subscribe(STOP)
        await pubsub.subscribe(STOP_ALL)

        async def listen():
            async for event in pubsub.listen():
                print(f'Redis Event: {event=}')
                if event['type'] == 'message':
                    data = json.loads(event['data'].decode())
                    channel = event['channel'].decode()
                    if channel == START:
                        on_start(data)

                    if channel == START_ALL:
                        pass  # START ALL

                    if channel == START_ALL:
                        pass  # STOP ALL

        # Höre auf redis nachrichten
        asyncio.create_task(listen())

        # Index schleife
        while True:
            pass


def main():
    """
    Einstiegspunkt für unser Skript
    """

    asyncio.run(execute())


if __name__ == "__main__":
    main()
