import json

import click


import asyncio

import aioredis
from messenger import Messenger


STATUS = "status"
ONLINE = "online"


INDEX = "index"
STOP = "stop"
START = "start"

redis = aioredis.from_url('redis://localhost')


async def pub_channel(channel, obj: object):
    print('PUB: ', channel, obj)
    return await redis.publish(channel, json.dumps(obj))


async def execute(symbol: str):
    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    test = await pub_channel(START, obj={'symbol': symbol})
    print(test)
    await asyncio.sleep(1)
    await redis.close()


@click.group()
def cli():
    pass


@cli.command()  # @cli, not @click!
@click.option(
    "--symbol",
    type=click.STRING
)
@click.option(
    "--state",
    type=click.STRING
)
def set(symbol, state):
    if state == '1':
        asyncio.run(execute(symbol))

    print(f'Enabling {symbol}')


@cli.command()  # @cli, not @click!
@click.option(
    "--symbol",
    type=click.STRING
)
def disable(symbol):
    print(f'Disable {symbol}')


if __name__ == "__main__":
    cli()
