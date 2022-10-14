import json

import click


import asyncio

import aioredis


START_ALL = "start_all"
STOP_ALL = "stop_all"


STOP = "stop"
START = "start"

redis = aioredis.from_url('redis://localhost')


async def pub_channel(channel, obj: object):
    print('PUB: ', channel, obj)
    return await redis.publish(channel, json.dumps(obj))


async def execute(channel, symbol: str=None):
    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    test = await pub_channel(channel, obj={'symbol': symbol})
    await asyncio.sleep(0.1)
    await redis.close()


@click.group()
def cli():
    pass


@cli.command()  # @cli, not @click!
@click.option("--symbol", type=click.STRING, required=False)

@click.option("--state", type=click.STRING)
@click.option("--all", type=click.BOOL, required=False)
def set(symbol, state, all):
    if symbol:
        if state == 'start':
            asyncio.run(execute(channel=START, symbol=symbol))
            print(f'STARTING {symbol}')
        if state == "stop":
            asyncio.run(execute(channel=STOP, symbol=symbol))
            print(f'STOPPING {symbol}')

    elif all:
        if state == 'start':
            asyncio.run(execute(channel=START_ALL))
            print(f'STARTING {symbol}')
        if state == "stop":
            asyncio.run(execute(channel=STOP_ALL))
            print(f'STOPPING {symbol}')


@cli.command()  # @cli, not @click!
@click.option("--symbol", type=click.STRING)
def disable(symbol):
    print(f'Disable {symbol}')


if __name__ == "__main__":
    cli()