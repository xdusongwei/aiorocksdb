import asyncio
import aiorocksdb.rocksdbapi
from aiorocksdb.meta import *
from aiorocksdb.batch import *
from aiorocksdb.key_lock import *
from aiorocksdb.list_command import ListCommand
from aiorocksdb.skip_list_command import SkipListCommand


class RocksDb:
    def __init__(self, path: str, create_if_missing: bool = True, use_executor: bool = False, *, loop=None):
        self._db = aiorocksdb.rocksdbapi.RocksDb(path, create_if_missing)
        self._loop = loop or asyncio.get_event_loop()
        self._key_lock = KeyLock()
        self.use_executor = use_executor

    async def __aenter__(self) -> 'RocksDb':
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def acquire(self, key):
        return self._key_lock.acquire(key)

    async def open(self):
        def _open(_db):
            is_ok = _db.open()
            return is_ok
        if self.use_executor:
            result = await self._loop.run_in_executor(None, _open, self._db)
        else:
            result = _open(self._db)
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

        if self.use_executor:
            result = await self._loop.run_in_executor(None, _get, self._db)
        else:
            result = _get(self._db)
        return result

    async def put(self, key: str, value: bytes):
        def _put(_db):
            is_ok = _db.put(key, value)
            return is_ok

        if self.use_executor:
            result = await self._loop.run_in_executor(None, _put, self._db)
        else:
            result = _put(self._db)
        return result

    async def delete(self, key: str):
        def _delete(_db):
            is_ok = _db.delete(key)
            return is_ok

        if self.use_executor:
            result = await self._loop.run_in_executor(None, _delete, self._db)
        else:
            result = _delete(self._db)
        return result

    async def execute(self, batch: Batch):
        def _execute(_db):
            is_ok = _db.execute(batch.batch)
            return is_ok

        result = await self._loop.run_in_executor(None, _execute, self._db)
        return result

    def batch_context(self):
        context = BatchContext(self)
        return context

    async def lpush(self, key: str, value: bytes):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                key_meta = KeyMeta()
                key_meta.key = key
                key_meta.key_type = KeyTypeEnum.LIST
            async with ListCommand(key, key_meta, self) as command:
                await command.insert(0, value)

    async def rpush(self, key: str, value: bytes):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                key_meta = KeyMeta()
                key_meta.key = key
                key_meta.key_type = KeyTypeEnum.LIST
            async with ListCommand(key, key_meta, self) as command:
                await command.insert(-1, value)

    async def lpop(self, key: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with ListCommand(key, key_meta, self) as command:
                value = await command.remove_by_index(0)
                return value

    async def rpop(self, key: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with ListCommand(key, key_meta, self) as command:
                value = await command.remove_by_index(-1)
                return value

    async def lindex(self, key: str, index: int):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with ListCommand(key, key_meta, self) as command:
                value = await command.index_of(index)
            return value

    async def llen(self, key: str) -> int:
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            if not key_meta:
                return 0
            return key_meta.length

    async def lset(self, key: str, index: int, value: bytes):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.LIST:
                raise ValueError(f'key {key} type is not LIST')
            async with ListCommand(key, key_meta, self) as command:
                await command.set(index, value)

    async def sadd(self, key: str, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_SET:
                raise ValueError(f'key {key} type is not SET')
            if not key_meta:
                key_meta = KeyMeta()
                key_meta.key = key
                key_meta.key_type = KeyTypeEnum.ORDER_LIST_SET
                key_meta.head_key = list()
                key_meta.tail_key = list()
                key_meta.seq = ''
            async with SkipListCommand(key, key_meta, self) as command:
                member = await command.insert(member, None)
            return member

    async def sismember(self, key: str, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_SET:
                raise ValueError(f'key {key} type is not SET')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with SkipListCommand(key, key_meta, self) as command:
                exists = await command.find_value_by_score(member)
                return bool(exists)

    async def smembers(self, key: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_SET:
                raise ValueError(f'key {key} type is not SET')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with SkipListCommand(key, key_meta, self) as command:
                score_list = await command.all_score()
                return score_list

    async def srem(self, key: str, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_SET:
                raise ValueError(f'key {key} type is not SET')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            async with SkipListCommand(key, key_meta, self) as command:
                exists = await command.remove_by_score(member)
                return exists

    async def delete_key(self, key: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if not key_meta:
                return False
            if key_meta.key_type == KeyTypeEnum.LIST:
                async with ListCommand(key, key_meta, self) as command:
                    await command.deconstruct()
                return True
            elif key_meta.key_type == KeyTypeEnum.ORDER_LIST:
                async with SkipListCommand(key, key_meta, self) as command:
                    await command.deconstruct()
            elif key_meta.key_type == KeyTypeEnum.ORDER_LIST_SET:
                async with SkipListCommand(key, key_meta, self) as command:
                    await command.deconstruct()
            elif key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
                async with SkipListCommand(key, key_meta, self) as command:
                    await command.deconstruct()
            else:
                return False

    async def zadd(self, key: str, score: int, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_ZSET:
                raise ValueError(f'key {key} type is not ZSET')
            if not key_meta:
                key_meta = KeyMeta()
                key_meta.key = key
                key_meta.key_type = KeyTypeEnum.ORDER_LIST_ZSET
                key_meta.head_key = list()
                key_meta.tail_key = list()
                key_meta.seq = (0, '')
            async with SkipListCommand(key, key_meta, self) as command:
                exists = await command.find_value_by_score(member)
                if exists:
                    await command.remove_by_score(exists)
                score = await command.insert((score, member), None)
            return score

    async def zcard(self, key: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_ZSET:
                raise ValueError(f'key {key} type is not ZSET')
            if not key_meta:
                return 0
            return key_meta.length

    async def zrem(self, key: str, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_ZSET:
                raise ValueError(f'key {key} type is not ZSET')
            if not key_meta:
                raise ValueError(f'key {key} not exists')
            value = False
            async with SkipListCommand(key, key_meta, self) as command:
                score = await command.find_value_by_score(member)
                if score:
                    value = await command.remove_by_score(score)
            return value

    async def zscore(self, key: str, member: str):
        async with self._key_lock.acquire(key):
            key_meta = await KeyMeta.find_meta(key, self)
            if key_meta and key_meta.key_type != KeyTypeEnum.ORDER_LIST_ZSET:
                raise ValueError(f'key {key} type is not ZSET')
            if not key_meta:
                key_meta = KeyMeta()
                key_meta.key = key
                key_meta.key_type = KeyTypeEnum.ORDER_LIST
                key_meta.head_key = list()
                key_meta.tail_key = list()
            value = False
            async with SkipListCommand(key, key_meta, self) as command:
                score = await command.find_value_by_score(member)
                if score:
                    value = score[0]
            return value


__all__ = ['RocksDb', ]
