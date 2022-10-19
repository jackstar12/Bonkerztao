import asyncio
import json
import math

import aioredis

redis = aioredis.from_url('redis://localhost')
pubsub = redis.pubsub()


INDEX = "index"


def join_args(*args):
    return ':'.join(args)


async def get_all_index_prices():
    # Hol index preis für das bestimmte symbol
    return {
        'btc': 20000,
        'eth': 1000
    }


async def pub_channel(channel, obj: object):
    print('PUB: ', channel, obj)
    return await redis.publish(channel, json.dumps(obj))


per_symbol_price = {}


async def call_forever(function):
    while True:
        try:
            await function()
        except Exception as e:
            print("ACHTUNG")

        await asyncio.sleep(2)


exchange_1_prev_index = {}


async def exchange_1():
    index_prices = await get_all_index_prices()

    for symbol, price in index_prices.items():
        per_symbol_price[symbol] = price
        prev_price = exchange_1_prev_index.get(symbol, 0)

        if price != prev_price:
            await redis.set(join_args(INDEX, symbol), price)
            await pub_channel(join_args(INDEX, symbol), {'price': price})
            exchange_1_prev_index[symbol] = price


async def get_exchange_2_index(symbol: str):
    return 10000  # beispiel 10000


exchange_2_prev_index = {}

all_symbols = ['btc', 'eth']


async def exchange_request_pro_symbol():

    for symbol in all_symbols:
        other_price = per_symbol_price.get(symbol)

        price = await get_exchange_2_index(symbol)
        prev_price = exchange_2_prev_index.get(symbol, 0)

        if price != prev_price:
            await redis.set(join_args(INDEX, symbol), price)
            await pub_channel(join_args(INDEX, symbol), {'price': price})

            exchange_2_prev_index[symbol] = price


async def main():
    """
    Entry
    """

    await asyncio.gather(
        call_forever(exchange_1),
        call_forever(exchange_request_pro_symbol)
    )


if __name__ == "__main__":
    asyncio.run(main())
