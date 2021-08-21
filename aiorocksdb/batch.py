from aiorocksdb.rocks_db import *
from aiorocksdb.error_type import *
from aiorocksdb.column_family import *


class Batch:
    def __init__(self, db, write_options: WriteOptions = None):
        d: RocksDb = db.d
        assert not d.is_readonly
        self.d = d
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
        item = ColumnFamilyBase.to_name(item)
        return BatchColumnFamily(item, self.d, self.batch)


class BatchContext:
    def __init__(self, db):
        self.db = db
        self.batch = Batch()

    async def __aenter__(self) -> 'Batch':
        return self.batch

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.db.execute(self.batch)
        self.db = None
        self.batch = None


__all__ = ['Batch', 'BatchContext', ]
