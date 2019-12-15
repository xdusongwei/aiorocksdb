import asyncio
import random
import pytest
from aiorocksdb import *


@pytest.mark.asyncio
async def test_list():
    async with RocksDb('test.db') as rocksdb:
        assert await rocksdb.llen('list') == 0
        await rocksdb.lpush('list', b'hello')
        await rocksdb.rpush('list', b'world')
        assert await rocksdb.llen('list') == 2
        await rocksdb.lpop('list')
        assert await rocksdb.llen('list') == 1
        assert await rocksdb.lindex('list', 0) == b'world'
        await rocksdb.rpop('list')
        assert await rocksdb.llen('list') == 0
        await rocksdb.delete_key('list')


@pytest.mark.asyncio
async def test_concurrent():
    job_count = 10
    async with RocksDb('test.db') as rocksdb:
        task = asyncio.gather(*[rocksdb.lpush('deplicate', b'node') for _ in range(job_count)])
        await task
        assert await rocksdb.llen('deplicate') == 10
        await rocksdb.delete_key('deplicate')


async def compare_nodes(key, rocksdb, compare_list):
    length = await rocksdb.llen(key)
    assert length == len(compare_list)
    for idx in range(length):
        db_value = await rocksdb.lindex(key, idx)
        assert db_value == compare_list[idx]


@pytest.mark.asyncio
async def test_monkey():
    async with RocksDb('test.db') as rocksdb:
        for _ in range(16):
            await rocksdb.delete_key('monkey_list')
            compare_list = list()
            for i in range(256):
                m = random.choice(['lpush', 'rpush', 'lpop', 'rpop', 'lset', 'delete'])
                if m == 'lpush':
                    value = str(random.randint(1, 9999999)).encode('utf8')
                    compare_list.insert(0, value)
                    await rocksdb.lpush('monkey_list', value)
                if m == 'rpush':
                    value = str(random.randint(1, 9999999)).encode('utf8')
                    compare_list.append(value)
                    await rocksdb.rpush('monkey_list', value)
                if m == 'lpop':
                    if not compare_list:
                        continue
                    compare_list.pop(0)
                    await rocksdb.lpop('monkey_list')
                if m == 'rpop':
                    if not compare_list:
                        continue
                    compare_list.pop(-1)
                    await rocksdb.rpop('monkey_list')
                if m == 'lset':
                    if not compare_list:
                        continue
                    idx = random.randint(0, len(compare_list) - 1)
                    value = str(random.randint(1, 9999999)).encode('utf8')
                    compare_list[idx] = value
                    await rocksdb.lset('monkey_list', idx, value)
                if m == 'delete':
                    compare_list.clear()
                    await rocksdb.delete_key('monkey_list')
                await compare_nodes('monkey_list', rocksdb, compare_list)
