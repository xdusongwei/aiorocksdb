import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_column_family():
    await RocksDb.destroy_db('db_test_native')

    user_cf = RColumnFamily('user')
    order_cf = RColumnFamily('order', ColumnFamilyOptions())
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    s = await RocksDb.open_db('db_test_native', option, [user_cf, order_cf, ])
    assert s.ok()
    r = s.result
    await r.put(b'userA', b'detailA', column_family=user_cf)
    s = await r.get(b'userA', column_family=user_cf)
    assert s.result == b'detailA'
    s = await r.drop_column_family(order_cf)
    assert s.ok()
    await r.close()

    s = await RocksDb.load_latest_options('db_test_native')
    assert s.ok()
    assert len(s.result) == 2
    options = s.options
    column_list = s.result
    s = await RocksDb.open_db('db_test_native', options, column_list)
    assert s.ok()
    r = s.result
    await r.close()

    await RocksDb.destroy_db('db_test_native')
