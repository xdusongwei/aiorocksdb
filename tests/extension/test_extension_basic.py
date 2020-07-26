import pytest
from aiorocksdb.rocks_db import *
from aiorocksdb.iterator import *
from aiorocksdb.batch import *
from aiorocksdb.extension import *


async def iterator(db, cf, d):
    # all keys
    match_seq = sorted(d.items(), key=lambda i: i[0])
    async with Iterator.range(cf) as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # all keys desc order
    match_seq = sorted(d.items(), key=lambda i: i[0], reverse=True)
    async with Iterator.range(cf, desc=True) as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # keys from start to stop
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if b'a3' <= k < b'b2']
    async with Iterator.range(cf, start=b'a3', stop=b'b1') as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # keys from start to last
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if b'a3' <= k]
    async with Iterator.range(cf, start=b'a3') as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # keys from first to stop
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if k < b'b2']
    async with Iterator.range(cf, stop=b'b1') as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # seek_to_prev [a4(a3), b1]
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if b'a3' <= k < b'b2']
    async with Iterator.range(cf, start=b'a4', stop=b'b1', seek_to_prev=True) as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # seek_to_prev [a3, b1]
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if b'a3' <= k < b'b2']
    async with Iterator.range(cf, start=b'a3', stop=b'b1', seek_to_prev=True) as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0

    # prefix
    match_seq = sorted(d.items(), key=lambda i: i[0])
    match_seq = [(k, v) for k, v in match_seq if k.startswith(b'b')]
    async with Iterator.prefix(cf, prefix=b'b') as it:
        async for k, v in it:
            assert (k, v) == match_seq[0]
            match_seq.pop(0)
    assert len(match_seq) == 0


@pytest.mark.asyncio
async def test_extension():
    await RocksDb.destroy_db('db_test_extension')

    option = Options()
    option.create_if_missing = True
    async with Db(Db.open_db('db_test_extension', option)) as db:
        cf = db['default']
        d = {
            b'a1': b'a1',
            b'a2': b'a2',
            b'a3': b'a3',
            b'a5': b'a5',
            b'b1': b'b1',
            b'b2': b'b2',
            b'b3': b'b3',
        }
        for k, v in d.items():
            await cf.put(k, v)

        async with Batch(db):
            cf = db['default']
            for k, v in d.items():
                await cf.put(k, v)

        await iterator(db, cf, d)

        async with Batch(db):
            cf = db['default']
            for k in d.keys():
                await cf.delete(k)

        cf = db['default']
        for k, v in d.items():
            await cf.delete(k)


@pytest.mark.asyncio
async def test_extension_readonly():
    await RocksDb.destroy_db('db_test_extension')

    option = Options()
    option.create_if_missing = True
    async with Db(Db.open_db('db_test_extension', option)):
        pass

    option = Options()
    async with Db(Db.open_db_for_readonly('db_test_extension', option)) as db:
        cf = db['default']
        with pytest.raises(AssertionError):
            await cf.put(b'readonly', b'readonly')
        with pytest.raises(AssertionError):
            await cf.delete(b'readonly')
        with pytest.raises(AssertionError):
            async with Batch(db):
                pass


@pytest.mark.asyncio
async def test_extension_column_family():
    await RocksDb.destroy_db('db_test_extension')

    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    user_cf = RColumnFamily('user')
    order_cf = RColumnFamily('order')
    async with Db(Db.open_db('db_test_extension', option, [user_cf, order_cf, ])) as db:
        status = await db.create_column_family('post')
        assert status.ok()

        status = await db.drop_column_family('post')
        assert status.ok()

        status = await db.drop_column_family('user')
        assert status.ok()

        status = await db.drop_column_family('order')
        assert status.ok()
