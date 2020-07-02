import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_snapshot():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_snapshot', option)
    assert s.ok()
    r = s.result
    s = await r.put(b'k', b'v')
    assert s.ok()
    snapshot = await r.create_snapshot()
    options = ReadOptions()
    snapshot.set_read_options(options)
    s = await r.get(b'k', options)
    assert s.ok()
    assert s.result == b'v'
    await r.release_snapshot(snapshot)
    await r.close()

