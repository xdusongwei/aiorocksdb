import asyncio
import pytest
from aiorocksdb import *


@pytest.mark.asyncio
async def test_command():
    async with RocksDb('test.db') as rocksdb:
        assert not (await rocksdb.get('new_key'))
        await rocksdb.put('new_key', b'new_value')
        assert (await rocksdb.get('new_key')) == b'new_value'
        await rocksdb.delete('new_key')


@pytest.mark.asyncio
async def test_batch():
    async with RocksDb('test.db') as rocksdb:
        batch = Batch()
        batch.put('batch_put', b'value')
        batch.delete('not_exists')
        await rocksdb.execute(batch)
        assert (await rocksdb.get('batch_put')) == b'value'


@pytest.mark.asyncio
async def test_batch_with():
    async with RocksDb('test.db') as rocksdb:
        async with BatchContext(rocksdb) as batch:
            batch.put('batch_context_put', b'value')
            batch.delete('batch_context_not_exists')
        assert (await rocksdb.get('batch_context_put')) == b'value'
