import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_sst_writer():
    sst_writer = SstFileWriter()
    s = await sst_writer.open('sst_writer.sst')
    assert s.ok()

    s = await sst_writer.put(b'ka', b'va')
    assert s.ok()
    s = await sst_writer.put(b'kb', b'vb')
    assert s.ok()
    s = await sst_writer.delete(b'kc')
    assert s.ok()
    s = await sst_writer.delete_range(b'ka', b'kb')
    assert s.ok()

    s = await sst_writer.finish()
    assert s.ok()


@pytest.mark.asyncio
async def test_sst_import():
    await RocksDb.destroy_db('db_test_native')

    sst_writer = SstFileWriter()
    s = await sst_writer.open('sst_import.sst')
    assert s.ok()

    s = await sst_writer.put(b'a', b'va')
    assert s.ok()
    s = await sst_writer.put(b'b', b'vb')
    assert s.ok()
    s = await sst_writer.put(b'c', b'vc')
    assert s.ok()

    s = await sst_writer.finish()
    assert s.ok()

    sst_reader = SstFileReader()
    s = await sst_reader.open('sst_import.sst')
    assert s.ok()
    s = await sst_reader.verify_checksum()
    assert s.ok()
    it = await sst_reader.create_iterator()
    await it.seek(b'a')
    valid = await it.valid()
    key = await it.key()
    assert key == b'a'
    value = await it.value()
    assert value == b'va'
    await it.close()
    await sst_reader.close()

    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_native', option)
    assert s.ok()
    r = s.result

    ingest_options = IngestExternalFileOptions()
    s = await r.ingest_external_file(['sst_import.sst', ], ingest_options)
    assert s.ok()

    s = await r.put(b'kz', b'vz')
    assert s.ok()
    s = await r.get(b'kz')
    assert s.ok() and s.result == b'vz'
    s = await r.get(b'a')
    assert s.ok() and s.result == b'va'
    s = await r.get(b'b')
    assert s.ok() and s.result == b'vb'

    await r.close()
