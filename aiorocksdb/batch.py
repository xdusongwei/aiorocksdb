import aiorocksdb.rocksdbapi


class Batch:
    def __init__(self):
        self.batch = aiorocksdb.rocksdbapi.RocksDbBatch()

    def put(self, key: str, value: bytes):
        return self.batch.put(key, value)

    def delete(self, key: str):
        return self.batch.delete(key)


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
