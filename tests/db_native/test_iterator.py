import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_iterator():
    await RocksDb.destroy_db('db_test_native')

    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_native', option)
    assert s.ok()
    r = s.result

    s = await r.put(b'a', b'va')
    assert s.ok()
    s = await r.put(b'b', b'vb')
    assert s.ok()
    s = await r.put(b'c', b'vc')
    assert s.ok()

    iterator = await r.create_iterator()

    await iterator.seek(b'b')
    valid = await iterator.valid()
    assert valid is True

    await iterator.next()
    valid = await iterator.valid()
    assert valid is True
    key = await iterator.key()
    assert key == b'c'
    value = await iterator.value()
    assert value == b'vc'

    await iterator.next()
    valid = await iterator.valid()
    assert valid is False
    s = await iterator.status()
    assert s.ok()

    await iterator.close()
    await r.close()
