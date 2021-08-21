from typing import *
from aiorocksdb.db_type import *
from aiorocksdb.codec import *
from aiorocksdb.rocks_db import *
from aiorocksdb.error_type import *
from aiorocksdb.column_family import *
from aiorocksdb.batch import *
from aiorocksdb.complex.codec import *
from aiorocksdb.complex.redis_style import *


class Snapshot:
    def __init__(self, db):
        self.d = db.d
        self.snapshot = None
        self.read_options = None

    async def __aenter__(self):
        rocksdb: RocksDb = self.d
        self.snapshot = await rocksdb.create_snapshot()
        self.read_options = ReadOptions()
        self.snapshot.set_read_options(self.read_options)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        rocksdb: RocksDb = self.d
        self.snapshot.clear_read_options(self.read_options)
        await rocksdb.release_snapshot(self.snapshot)
        self.snapshot = None
        self.read_options = None

    def __getitem__(self, item) -> SnapshotColumnFamily:
        item = ColumnFamilyBase.to_name(item)
        return SnapshotColumnFamily(item, self.d, self.read_options)


class Db:
    open_db = RocksDb.open_db
    open_ttl_db = RocksDb.open_db
    open_db_for_readonly = RocksDb.open_db_for_readonly
    open_transaction_db = RocksDb.open_transaction_db
    open_optimistic_transaction_db = RocksDb.open_optimistic_transaction_db

    def __init__(self, open_handler, codec_list=None):
        self.open_handler = open_handler
        self.d: RocksDb = None
        self.codec_list = codec_list

    @property
    def is_open(self):
        return bool(self.d)

    def __getitem__(self, item) -> ColumnFamily:
        item = ColumnFamilyBase.to_name(item)
        return ColumnFamily(item, self.d)

    async def open(self):
        assert self.d is None
        status: StatusT[RocksDb] = await self.open_handler
        if status.ok():
            self.d = status.result
            self.codec_list = self.codec_list or list()
            self.codec_list.append(ComplexCodec())
            self.codec_list.append(Codec(None))
            self.d.codec_list = self.codec_list
        else:
            raise StatusError(status)

    async def close(self):
        if self.d is None:
            return
        await self.d.close()
        self.d = None

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def create_column_family(self, column_family: Union[str, RColumnFamilyT, ColumnFamilyBase], options: ColumnFamilyOptions = None) -> StatusT:
        cf = RColumnFamily(column_family)
        status = await self.d.create_column_family(cf, options)
        return status

    async def drop_column_family(self, column_family: Union[str, RColumnFamilyT, ColumnFamilyBase]) -> StatusT:
        name = ColumnFamilyBase.to_name(column_family)
        cf = ColumnFamily(name, self.d).cf
        status = await self.d.drop_column_family(cf)
        return status

    def redis(self, column_family: Union[str, RColumnFamilyT, ColumnFamilyBase]) -> RedisCommand:
        name = ColumnFamilyBase.to_name(column_family)
        cf = ColumnFamily(name, self.d)
        command = RedisCommand(cf, self)
        return command

    @property
    def interface(self):
        return self.d


__all__ = [
    'Db',
    'Snapshot',
]
