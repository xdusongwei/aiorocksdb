import asyncio
import aiorocksdb.rocksdbapi


class RocksDbBatch:
    def __init__(self):
        self.batch = aiorocksdb.rocksdbapi.RocksDbBatch()

    def put(self, key: str, value: bytes):
        return self.batch.put(key, value)

    def delete(self, key: str):
        return self.batch.delete(key)


class RocksDb:
    def __init__(self, path: str, create_if_missing: bool = True, *, loop=None):
        self._db = aiorocksdb.rocksdbapi.RocksDb(path, create_if_missing)
        self._loop = loop or asyncio.get_event_loop()

    async def __aenter__(self) -> 'RocksDb':
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def open(self):
        def _open(_db):
            is_ok = _db.open()
            return is_ok

        result = await self._loop.run_in_executor(None, _open, self._db)
        return result

    async def close(self):
        def _close(_db):
            _db.close()

        if self._db:
            await self._loop.run_in_executor(None, _close, self._db)
        self._db = None

    async def get(self, key: str):
        def _get(_db):
            value = _db.get(key)
            return value

        result = await self._loop.run_in_executor(None, _get, self._db)
        return result

    async def put(self, key: str, value: bytes):
        def _put(_db):
            is_ok = _db.put(key, value)
            return is_ok

        result = await self._loop.run_in_executor(None, _put, self._db)
        return result

    async def delete(self, key: str):
        def _delete(_db):
            is_ok = _db.delete(key)
            return is_ok

        result = await self._loop.run_in_executor(None, _delete, self._db)
        return result

    async def execute(self, batch: RocksDbBatch):
        def _execute(_db):
            is_ok = _db.execute(batch.batch)
            return is_ok

        result = await self._loop.run_in_executor(None, _execute, self._db)
        return result


__all__ = ['RocksDb', ]
