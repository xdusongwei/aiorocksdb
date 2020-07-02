import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_column_family():
    user_cf = RColumnFamily('user')
    order_cf = RColumnFamily('order', ColumnFamilyOptions())
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    s = await RocksDb.open_db('db_test_column_family', option, [user_cf, order_cf])
    assert s.ok()
    r = s.result
    await r.put(b'userA', b'detailA', column_family=user_cf)
    s = await r.get(b'userA', column_family=user_cf)
    assert s.result == b'detailA'
    s = await r.drop_column_family(order_cf)
    assert s.ok()
    s = await r.create_column_family(order_cf)
    assert s.ok()
    await r.close()

    s = await RocksDb.load_latest_options('db_test_column_family')
    assert s.ok()
    assert len(s.result) == 3
    options = s.options
    column_list = s.result
    s = await RocksDb.open_db('db_test_column_family', options, column_list)
    assert s.ok()
    r = s.result
    await r.close()
