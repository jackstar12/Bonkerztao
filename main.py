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


STOP = "stop"
START = "start"

START_ALL = "start_all"
STOP_ALL = "stop_all"


def join_args(*args):
    return ':'.join(args)


async def ob_run(stock: str):

    # Auth WS

    # Setup Redis pubsub
    await pubsub.subscribe(STOP)
    await pubsub.subscribe(STOP_ALL)

    async def listen_pubsub():
        async for event in pubsub.listen():
            print(f'Redis Event: {event=}')
            if event['type'] == 'message':
                data = json.loads(event['data'].decode())
                channel = event['channel'].decode()
                if channel == STOP:
                    if data['symbol'] == stock:
                        pass  # STOP!
                elif channel == STOP_ALL:
                    pass

    asyncio.create_task(listen_pubsub())

    while True:  # Listen WS
        msg = 'moin moin'
        print(f'WEBSOCKET MESSAGE FROM {stock}: {msg}')
        await asyncio.sleep(1)


def test_ob(stock: str, config):
    asyncio.run(ob_run(stock))


def main_bot(stock: str):
    stock_config = importlib.import_module(f'bots.{stock}.config')
    test_ob(stock, stock_config)


async def watch(executor, stock: str):
    while True:
        print(f'Starting stock {stock}')

        await redis.set(join_args(STATUS, stock), 1)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, main_bot, stock)

        print(f'Stock {stock} finished, restarting, Result: {result}')


async def execute():
    result = await redis.ping()
    if not result:
        print('REDIS CONNECTION NOT WORKING!')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Main (manager von spezifischen prozessen)
        # Subscribe = Empfangen

        symbols_running = []

        def on_start(data: dict):
            symbol = data['symbol']
            if symbol not in symbols_running:
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

                symbols_running.append(symbol)
                asyncio.create_task(watch(executor, symbol))
            else:
                print(f"{symbol} läuft schon")

        await pubsub.subscribe(START)
        await pubsub.subscribe(START_ALL)
        await pubsub.subscribe(STOP)
        await pubsub.subscribe(STOP_ALL)

        async def listen_pubsub():
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
        await listen_pubsub()


def main():
    """
    Einstiegspunkt für unser Skript
    """

    asyncio.run(execute())


if __name__ == "__main__":
    main()
