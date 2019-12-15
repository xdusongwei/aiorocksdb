import asyncio
import random
import pytest
from aiorocksdb import *


@pytest.mark.asyncio
async def test_list():
    key = 'skipListComplex'
    async with RocksDb('test.db') as rocksdb:
        await rocksdb.delete_key(key)
        assert await rocksdb.zcard(key) == 0
        await rocksdb.zadd(key, 1, 'abc')
        assert await rocksdb.zcard(key) == 1
        assert await rocksdb.zscore(key, 'abc') == 1
        await rocksdb.zrem(key, 'abc')
        assert await rocksdb.zcard(key) == 0
        await rocksdb.delete_key(key)


@pytest.mark.asyncio
async def test_member_change_score():
    key = 'skipListComplexChangeScore'
    async with RocksDb('test.db') as rocksdb:
        await rocksdb.delete_key(key)
        assert await rocksdb.zcard(key) == 0
        await rocksdb.zadd(key, 1, 'abc')
        await rocksdb.zadd(key, 2, 'abc')
        assert await rocksdb.zcard(key) == 1
        await rocksdb.delete_key(key)
