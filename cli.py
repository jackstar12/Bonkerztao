import click


import asyncio

import aioredis
from messenger import Messenger, Channel


async def execute(symbol: str):
    redis = aioredis.from_url('redis://localhost')
    messenger = Messenger(redis)
    print('hi')
    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    await messenger.pub_channel(Channel.START, obj={'symbol': symbol})


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
