import asyncio

import aioredis
from messenger import Messenger


def on_start(event: dict):
    print('Starting', event['symbol'])


async def listen(pubsub):
    async for msg in pubsub.listen():
        print('MSG', msg)


async def execute():
    redis = aioredis.from_url('redis://localhost')
    messenger = Messenger(redis)

    result = await redis.ping()
    if not result:
        print('REDIS CONNECTION NOT WORKING!')

    # Main (manager von spezifischen prozessen)
    # Subscribe = Empfangen
    await messenger.sub_channel(Channel.START, callback=on_start)

    await asyncio.sleep(1)

    # CLI (manuell soll gestartet werden)
    # Publish = Veröffentlichen / Senden
    await messenger.pub_channel(Channel.START, obj={'symbol': 'BTC'})

    await asyncio.sleep(20)
    await redis.close()


if __name__ == "__main__":
    asyncio.run(execute())
