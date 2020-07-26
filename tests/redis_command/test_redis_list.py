import pytest
from aiorocksdb.rocks_db import *
from aiorocksdb.extension import *


@pytest.mark.asyncio
async def test_list_commands():
    await RocksDb.destroy_db('db_test_redis')

    option = Options()
    option.create_if_missing = True
    async with Db(Db.open_db('db_test_redis', option)) as db:
        redis = db.redis('default')
        length = await redis.llen(b'notExist')
        assert length == 0

        await redis.lpush(b'list', b'first')
        length = await redis.llen(b'list')
        assert length == 1
        v = await redis.lindex(b'list', 0)
        assert v == b'first'
        v = await redis.lindex(b'list', -1)
        assert v == b'first'

        await redis.rpush(b'list', b'second')
        length = await redis.llen(b'list')
        assert length == 2
        v = await redis.lindex(b'list', 0)
        assert v == b'first'
        v = await redis.lindex(b'list', 1)
        assert v == b'second'

        await redis.lset(b'list', 0, b'newFirst')
        v = await redis.lindex(b'list', 0)
        assert v == b'newFirst'
        await redis.lset(b'list', 0, b'first')

        v = await redis.rpop(b'list')
        assert v == b'second'
        length = await redis.llen(b'list')
        assert length == 1
        v = await redis.lpop(b'list')
        assert v == b'first'


@pytest.mark.asyncio
async def test_list_delete():
    await RocksDb.destroy_db('db_test_redis')
    option = Options()
    option.create_if_missing = True
    async with Db(Db.open_db('db_test_redis', option)) as db:
        redis = db.redis('default')
        await redis.rpush(b'list', b'first')
        await redis.rpush(b'list', b'second')

        await redis.delete(b'list')
