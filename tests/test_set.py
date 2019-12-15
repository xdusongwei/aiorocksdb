import asyncio
import random
import pytest
from aiorocksdb import *


@pytest.mark.asyncio
async def test_list():
    key = 'set'
    async with RocksDb('test.db') as rocksdb:
        await rocksdb.delete_key(key)
        with pytest.raises(ValueError):
            assert not await rocksdb.sismember(key, '1234')
        assert await rocksdb.sadd(key, '1234')
        assert await rocksdb.sismember(key, '1234')
        assert await rocksdb.srem(key, '1234')
        assert not await rocksdb.srem(key, '1234')
        assert not await rocksdb.srem(key, '12345')
        assert await rocksdb.smembers(key) == list()
        assert await rocksdb.sadd(key, 'new_member')
        assert await rocksdb.smembers(key) == ['new_member', ]
        await rocksdb.delete_key(key)

