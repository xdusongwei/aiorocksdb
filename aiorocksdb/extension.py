from typing import *
import abc
from .db_type import *
from .rocks_db import *


class StatusError(Exception):
    def __init__(self, status):
        super(StatusError, self).__init__()
        self.status: StatusT = status

    def __str__(self):
        return f'<StatusError code:{self.status.code()} subCode:{self.status.subcode()} {self.status.ToString()}>'


class ColumnFamilyBase(abc.ABC):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        cf_dict = d.column_family_dict
        cf_name = cf_name or d.DEFAULT_COLUMN_FAMILY
        assert cf_name in cf_dict
        self.d = d
        self.cf = cf_dict[cf_name]
        self.name = cf_name
        self.read_options = read_options


class ColumnFamily(ColumnFamilyBase):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        super(ColumnFamily, self).__init__(cf_name=cf_name, d=d, read_options=read_options)

    def __str__(self):
        return f'<ColumnFamily {self.name}>'

    async def get(self, key: bytes, raise_exception=False) -> Optional[bytes]:
        status = await self.d.get(key, column_family=self.cf, read_options=self.read_options)
        if status.ok():
            return status.result
        else:
            if raise_exception:
                raise StatusError(status)
            else:
                return None

    async def put(self, key: bytes, value: bytes):
        status = await self.d.put(key, value, column_family=self.cf)
        if not status.ok():
            raise StatusError(status)

    async def delete(self, key: bytes):
        status = await self.d.delete(key, column_family=self.cf)
        if not status.ok():
            raise StatusError(status)


class SnapshotColumnFamily(ColumnFamilyBase):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        super(SnapshotColumnFamily, self).__init__(cf_name=cf_name, d=d, read_options=read_options)

    def __str__(self):
        return f'<SnapshotColumnFamily {self.name}>'

    async def get(self, key: bytes, raise_exception=False) -> Optional[bytes]:
        status = await self.d.get(key, column_family=self.cf, read_options=self.read_options)
        if status.ok():
            return status.result
        else:
            if raise_exception:
                raise StatusError(status)
            else:
                return None


class BatchColumnFamily(ColumnFamilyBase):
    def __init__(self, cf_name: str, d: RocksDb, batch: RBatch):
        super(BatchColumnFamily, self).__init__(cf_name=cf_name, d=d)
        self.batch = batch

    def __str__(self):
        return f'<BatchColumnFamily {self.name}>'

    def put(self, key: bytes, value: bytes):
        self.batch.put(key, value, self.cf)

    def delete(self, key: bytes):
        self.batch.delete_key(key, self.cf)


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
        return SnapshotColumnFamily(item, self.d, self.read_options)


class Batch:
    def __init__(self, db, write_options: WriteOptions = None):
        self.d = db.d
        self.batch = None
        self.write_options = write_options

    async def __aenter__(self):
        self.batch = RBatch()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        rocksdb: RocksDb = self.d
        status = await rocksdb.write(self.batch, self.write_options)
        if not status.ok():
            raise StatusError(status)

    def __getitem__(self, item) -> BatchColumnFamily:
        return BatchColumnFamily(item, self.d, self.batch)


class Db:
    open_db = RocksDb.open_db
    open_ttl_db = RocksDb.open_db
    open_db_for_readonly = RocksDb.open_db_for_readonly
    open_transaction_db = RocksDb.open_transaction_db
    open_optimistic_transaction_db = RocksDb.open_optimistic_transaction_db

    def __init__(self, open_handler):
        self.open_handler = open_handler
        self.d: RocksDb = None

    @property
    def is_open(self):
        return bool(self.d)

    def __getitem__(self, item) -> ColumnFamily:
        return ColumnFamily(item, self.d)

    async def open(self):
        assert self.d is None
        status: StatusT = await self.open_handler
        if status.ok():
            self.d = status.result
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


__all__ = ['StatusError', 'Db', 'ColumnFamily', 'Snapshot', 'SnapshotColumnFamily', 'BatchColumnFamily', 'Batch', ]
