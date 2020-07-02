import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_open_db_not_exists():
    option = Options()
    s = await RocksDb.open_db('db_test_not_exist', option)
    assert not s.ok()
    assert s.code() == 4


@pytest.mark.asyncio
async def test_open_db_exists():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_exist', option)
    assert s.ok()
    r = s.result
    s = await r.flush([r.default_column_family, ])
    assert s.ok()
    await r.close()

    s = await RocksDb.repair_db('db_test_exist')
    assert s.ok()
    s = await RocksDb.destroy_db('db_test_exist')
    assert s.ok()


@pytest.mark.asyncio
async def test_open_transaction_db():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_transaction_db('db_test_transaction', option)
    assert s.ok()
    r = s.result
    await r.close()


@pytest.mark.asyncio
async def test_open_optimistic_transaction_db():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_optimistic_transaction_db('db_test_optimistic_transaction', option)
    assert s.ok()
    r = s.result
    await r.close()
