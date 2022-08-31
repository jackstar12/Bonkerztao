import json

import click


import asyncio

import aioredis


STATUS = "status"
ONLINE = "online"


INDEX = "index"
START = "start"
STOP = "stop"

redis = aioredis.from_url('redis://localhost')


async def pub_channel(channel, obj: object):
    print('PUB: ', channel, obj)
    return await redis.publish(channel, json.dumps(obj))


async def enable_symbol(symbol: str):
    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    test = await pub_channel(START, obj={'symbol': symbol})
    await asyncio.sleep(1)
    await redis.close()


async def disable_symbol(symbol: str):
    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    test = await pub_channel(STOP, obj={'symbol': symbol})
    await asyncio.sleep(0.1)
    await redis.close()


@click.group()
def cli():
    pass


@cli.command()  # @cli, not @click!
@click.option(
    "--symbol",
    type=click.STRING
)
def enable(symbol):
    asyncio.run(enable_symbol(symbol))

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
