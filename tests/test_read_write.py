import asyncio
import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_read_write():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_read_write', option)
    assert s.ok()
    r = s.result

    s = await r.put(b'ka', b'va')
    assert s.ok()
    s = await r.put(b'kb', b'vb')
    assert s.ok()

    s = await r.get(b'ka')
    assert s.ok() and s.result == b'va'
    s = await r.get(b'kb')
    assert s.ok() and s.result == b'vb'

    s = await r.delete(b'ka')
    assert s.ok()

    s = await r.delete_range(b'ka', b'kz')
    assert s.ok()

    s = await r.get(b'ka')
    assert s.code() == 1 and s.result is None
    s = await r.get(b'kb')
    assert s.code() == 1 and s.result is None

    batch = RBatch()
    batch.put(b'kx', b'vx', r.default_column_family)
    batch.put(b'ky', b'vy', r.default_column_family)
    batch.delete_key(b'kz', r.default_column_family)
    s = await r.write(batch)
    assert s.ok()

    await r.close()

    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_read_write', option)
    assert s.ok()
    r = s.result

    s = await RocksDb.open_db_for_readonly('db_test_read_write', option)
    assert s.ok()
    r_readonly = s.result

    s = await r_readonly.put(b'ka', b'va')
    assert not s.ok()

    await r.close()
    await r_readonly.close()


@pytest.mark.asyncio
async def test_read_write_ttl():
    option = Options()
    option.create_if_missing = True
    column_family_list = [RColumnFamily(RocksDb.DEFAULT_COLUMN_FAMILY)]
    s = await RocksDb.open_ttl_db('db_test_read_write_ttl', ttls=[1], column_family_list=column_family_list, options=option)
    assert s.ok()
    r = s.result
    await r.set_ttl(1)
    await r.set_column_family_ttl(r.column_family_list[0], 1)
    s = await r.put(b'ka', b'va')
    assert s.ok()
    s = await r.get(b'ka')
    assert s.ok() and s.result == b'va'
    await r.close()
