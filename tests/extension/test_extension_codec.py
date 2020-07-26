import json
import pytest
import toml
import msgpack
from aiorocksdb.rocks_db import *
from aiorocksdb.codec import *
from aiorocksdb.iterator import *
from aiorocksdb.batch import *
from aiorocksdb.extension import *


class TomlCodec(Codec):
    def loads(self, data: bytes):
        data = data.decode('utf8')
        return toml.loads(data)

    def dumps(self, data):
        return toml.dumps(data).encode('utf8')


class JsonCodec(Codec):
    def loads(self, data: bytes):
        data = data.decode('utf8')
        return json.loads(data)

    def dumps(self, data):
        return json.dumps(data).encode('utf8')


d = {
    b'fruit:apple': {
        'color': 'red',
        'qty': 1,
    },
    b'fruit:orange': {
        'color': 'yellow',
        'qty': 2,
    },
    b'device:phone': {
        'network': 'WiFi',
        'memory': 4096,
    },
    b'device:switch': {
        'network': 'LAN',
        'memory': 256,
    },
    b'animal:dog': {
        'color': 'black',
        'legs': 4,
    },
    b'animal:cat': {
        'color': 'white',
        'legs': 4,
    },
    b'foo': b'bar',
}


@pytest.mark.asyncio
async def test_codec_basic():
    await RocksDb.destroy_db('db_test_extension_codec')

    global d
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    codec_cf = RColumnFamily('codec')
    codec_fruit = TomlCodec(b'fruit')
    codec_device = Codec(b'device', msgpack.loads, msgpack.dumps)
    codec_animal = JsonCodec(b'animal')
    codec_list = [codec_fruit, codec_device, codec_animal, ]
    async with Db(Db.open_db('db_test_extension_codec', option, [codec_cf, ]), codec_list) as db:
        cf = db[codec_cf]

        for k, v in d.items():
            await cf.put(k, v)
        orange = await cf.get(b'fruit:orange')
        assert orange == {
            'color': 'yellow',
            'qty': 2,
        }
        phone = await cf.get(b'device:phone')
        assert phone == {
            'network': 'WiFi',
            'memory': 4096,
        }
        dog = await cf.get(b'animal:dog')
        assert dog == {
            'color': 'black',
            'legs': 4,
        }
        bar = await cf.get(b'foo')
        assert bar == b'bar'


@pytest.mark.asyncio
async def test_codec_snapshot():
    await RocksDb.destroy_db('db_test_extension_codec')

    global d
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    codec_cf = RColumnFamily('codec')
    codec_fruit = TomlCodec(b'fruit')
    codec_device = Codec(b'device', msgpack.loads, msgpack.dumps)
    codec_animal = JsonCodec(b'animal')
    codec_list = [codec_fruit, codec_device, codec_animal, ]
    async with Db(Db.open_db('db_test_extension_codec', option, [codec_cf, ]), codec_list) as db:
        cf = db[codec_cf]
        for k, v in d.items():
            await cf.put(k, v)
        async with Snapshot(db) as snapshot:
            cf = snapshot[codec_cf]
            orange = await cf.get(b'fruit:orange')
            assert orange == {
                'color': 'yellow',
                'qty': 2,
            }
            phone = await cf.get(b'device:phone')
            assert phone == {
                'network': 'WiFi',
                'memory': 4096,
            }
            dog = await cf.get(b'animal:dog')
            assert dog == {
                'color': 'black',
                'legs': 4,
            }
            bar = await cf.get(b'foo')
            assert bar == b'bar'


@pytest.mark.asyncio
async def test_codec_iterator():
    await RocksDb.destroy_db('db_test_extension_codec')

    global d
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    codec_cf = RColumnFamily('codec')
    codec_fruit = TomlCodec(b'fruit')
    codec_device = Codec(b'device', msgpack.loads, msgpack.dumps)
    codec_animal = JsonCodec(b'animal')
    codec_list = [codec_fruit, codec_device, codec_animal, ]
    async with Db(Db.open_db('db_test_extension_codec', option, [codec_cf, ]), codec_list) as db:
        cf = db[codec_cf]
        for k, v in d.items():
            await cf.put(k, v)

        match_seq = sorted(d.items(), key=lambda i: i[0])
        match_seq = [(k, v) for k, v in match_seq if k.startswith(b'fruit')]
        async with Iterator.prefix(cf, b'fruit') as iter:
            async for k, v in iter:
                assert k == match_seq[0][0]
                assert v == match_seq[0][1]
                match_seq.pop(0)
        assert len(match_seq) == 0


@pytest.mark.asyncio
async def test_codec_batch():
    await RocksDb.destroy_db('db_test_extension_codec')

    global d
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    codec_cf = RColumnFamily('codec')
    codec_fruit = TomlCodec(b'fruit')
    codec_device = Codec(b'device', msgpack.loads, msgpack.dumps)
    codec_animal = JsonCodec(b'animal')
    codec_list = [codec_fruit, codec_device, codec_animal, ]
    async with Db(Db.open_db('db_test_extension_codec', option, [codec_cf, ]), codec_list) as db:
        async with Batch(db) as batch:
            cf = batch[codec_cf]
            for k, v in d.items():
                cf.put(k, v)
