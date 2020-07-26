import pytest
from aiorocksdb.rocks_db import *
from aiorocksdb.extension import *


@pytest.mark.asyncio
async def test_string_commands():
    await RocksDb.destroy_db('db_test_redis')

    option = Options()
    option.create_if_missing = True
    async with Db(Db.open_db('db_test_redis', option)) as db:
        redis = db.redis('default')
        v = await redis.get(b'notExist')
        assert v is None

        await redis.set(b'foo', b'bar')
        v = await redis.get(b'foo')
        assert v == b'bar'
        await redis.delete(b'foo')
        v = await redis.get(b'foo')
        assert v is None
