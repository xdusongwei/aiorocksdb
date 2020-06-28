import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_transaction():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_transaction_db('db_test_transaction', option)
    assert s.ok()
    r = s.result

    transaction = await r.begin_transaction()
    s = await transaction.put(b'ka', b'va')
    assert s.ok()
    s = await transaction.commit()
    assert s.ok()
    await r.release_transaction(transaction)

    transaction_first = await r.begin_transaction()
    transaction_last = await r.begin_transaction()
    s = await transaction_first.put(b'ka', b'va')
    assert s.ok()
    s = await transaction_last.delete(b'ka')
    assert not s.ok()
    s = await transaction_first.commit()
    assert s.ok()
    await r.release_transaction(transaction_first)
    await r.release_transaction(transaction_last)

    transaction = await r.begin_transaction()
    await transaction.set_save_point()
    s = await transaction.put(b'ka', b'va2')
    assert s.ok()
    s = await transaction.rollback()
    assert s.ok()
    await r.release_transaction(transaction)
    s = await r.get(b'ka')
    assert s.ok() and s.result == b'va'

    await r.close()


@pytest.mark.asyncio
async def test_optimistic_transaction():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_optimistic_transaction_db('db_test_optimistic_transaction', option)
    assert s.ok()
    r = s.result

    transaction = await r.begin_optimistic_transaction()
    s = await transaction.put(b'ka', b'va')
    assert s.ok()
    s = await transaction.commit()
    assert s.ok()
    await r.release_transaction(transaction)

    transaction_first = await r.begin_optimistic_transaction()
    transaction_last = await r.begin_optimistic_transaction()
    s = await transaction_first.put(b'ka', b'va')
    assert s.ok()
    s = await transaction_last.delete(b'ka')
    assert s.ok()
    s = await transaction_first.commit()
    assert s.ok()
    s = await transaction_last.commit()
    assert not s.ok()
    await r.release_transaction(transaction_first)
    await r.release_transaction(transaction_last)

    await r.close()

