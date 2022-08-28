import importlib
import concurrent.futures

import click
import time

import asyncio
import logging
import aioredis

from messenger import Channel, Messenger, join_args
from redis import redis, Namespace

STOCKS = ['eth']


def open_order_update_main(stock: str):
    stock_config = importlib.import_module(f'bots.{stock}.config')

    print('moin moin')
    time.sleep(stock_config.SLEEP_TIME)
    return True


async def watch(executor, stock: str):
    while True:
        print(f'Starting stock {stock}')

        #await redis.set(join_args(Namespace.STATUS), 1)
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, open_order_update_main, stock)

        print(f'Stock {stock} finished, restarting, Result: {result}')


async def execute():
    messenger = Messenger(redis)

    result = await redis.ping()
    if not result:
        print('REDIS CONNECTION NOT WORKING!')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        all_tasks = []

        # Main (manager von spezifischen prozessen)
        # Subscribe = Empfangen

        def start_stock(symbol: str):
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

        def on_start(data: dict):
            start_stock(data['symbol'])

        await messenger.sub_channel(Channel.START, callback=on_start)

        for stock in STOCKS:
            start_stock(stock)
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
