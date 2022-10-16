import asyncio
import json

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


async def main():
    """
    Entry
    """
    prev_index = {}

    while True:
        index_prices = await get_all_index_prices()

        for symbol, price in index_prices.items():
            prev_price = prev_index.get(symbol, 0)

            if price != prev_price:
                await redis.set(join_args(INDEX, symbol), price)
                await pub_channel(join_args(INDEX, symbol), {'price': price})

                prev_index[symbol] = price

        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
