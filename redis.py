import aioredis
from enum import Enum

redis = aioredis.from_url('redis://localhost')


class Namespace(Enum):
    STATUS = "status"
    ONLINE = "online"
