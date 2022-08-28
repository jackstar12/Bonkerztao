import asyncio
import json
import logging
from enum import Enum
from functools import wraps
from typing import Dict

from aioredis import Redis


class Channel(Enum):
    INDEX = "index"
    STOP = "stop"
    START = "start"


def join_args(*args):
    return ':'.join(str(arg) for arg in args if arg)


class Messenger:

    def __init__(self, redis: Redis):
        self._redis = redis
        self._pubsub = self._redis.pubsub()
        self._listening = False
        self._logger = logging.getLogger('Messenger')

    def _wrap(self, coro, rcv_event=False):
        @wraps(coro)
        def wrapper(event: Dict, *args, **kwargs):
            self._logger.info(f'Redis Event: {event=} {args=} {kwargs=}')
            if rcv_event:
                data = event
            else:
                data = json.loads(event['data'].decode('utf-8'))
            coro(data)

        return wrapper

    async def listen(self):
        logging.info('Started Listening.')
        async for msg in self._pubsub.listen():
            logging.info(f'MSG {msg}')

    async def sub(self, pattern=False, **kwargs):
        if pattern:
            await self._pubsub.psubscribe(**kwargs)
        else:
            await self._pubsub.subscribe(**kwargs)
        if not self._listening:
            self._listening = True
            asyncio.create_task(self.listen())

    async def unsub(self, channel: str, is_pattern=False):
        if is_pattern:
            await self._pubsub.punsubscribe(channel)
        else:
            await self._pubsub.unsubscribe(channel)

    async def sub_channel(self, category: Channel, channel_id: int = None, callback = None,
                          pattern=False):
        channel = join_args(category.value, channel_id)
        if pattern:
            channel += '*'
        kwargs = {channel: self._wrap(callback)}
        logging.info(f'Sub: {kwargs}')
        await self.sub(pattern=pattern, **kwargs)

    async def unsub_channel(self, category: Channel, channel_id: int = None, pattern=False):
        channel = join_args(category.value, channel_id)
        await self.unsub(channel, pattern)

    async def pub_channel(self, channel: Channel, obj: object, channel_id: int = None):
        ch = join_args(channel.value, channel_id)
        return await self._redis.publish(ch, json.dumps(obj))
