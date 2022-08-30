import asyncio
import json
import logging
from enum import Enum
from functools import wraps
from typing import Dict

from aioredis import Redis


def join_args(*args):
    return ':'.join([str(arg) for arg in args if arg])


class Messenger:

    def __init__(self, redis: Redis):
        self._redis = redis
        self._pubsub = self._redis.pubsub()
        self._listening = False
        self._logger = logging.getLogger('Messenger')

    def _wrap(self, callback, rcv_event=False):
        @wraps(callback)
        def wrapper(event: Dict, *args, **kwargs):
            self._logger.info(f'Redis Event: {event=} {args=} {kwargs=}')
            if rcv_event:
                data = event
            else:
                data = json.loads(event['data'].decode('utf-8'))
            callback(data)

        return wrapper

    async def listen(self):
        logging.info('Started Listening.')
        async for msg in self._pubsub.listen():
            logging.info(f'MSG {msg}')

    async def sub(self, **kwargs):
        await self._pubsub.subscribe(**kwargs)
        if not self._listening:
            self._listening = True
            asyncio.create_task(self.listen())

    async def sub_channel(self, channel, channel_id: int = None, callback = None):
        channel = join_args(channel, channel_id)
        kwargs = {channel: self._wrap(callback)}
        logging.info(f'Sub: {kwargs}')
        await self.sub(**kwargs)

    async def unsub_channel(self, channel, channel_id: int = None):
        channel = join_args(channel.value, channel_id)
        await self._pubsub.unsubscribe(channel)

    async def pub_channel(self, channel, obj: object, channel_id: int = None):
        ch = join_args(channel, channel_id)
        return await self._redis.publish(ch, json.dumps(obj))
