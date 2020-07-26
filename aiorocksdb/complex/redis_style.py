from typing import *
from aiorocksdb.key_lock import *
from aiorocksdb.meta import KeyMeta, KeyTypeEnum
from aiorocksdb.complex.codec import *
from aiorocksdb.batch import *
from aiorocksdb.column_family import *
from aiorocksdb.complex.node_base import *
from aiorocksdb.complex.linked_list import *


class RedisCommand(NodeBase):
    LOCK = KeyLock()

    def __init__(self, cf: ColumnFamily, db):
        self.cf = cf
        self.db = db

    async def get(self, key):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        data_key = codec.create_data_key(key, )
        async with RedisCommand.LOCK.acquire(key):
            await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.STRING)
            value = await self.cf.get(data_key)
            return value

    async def set(self, key, value):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        data_key = codec.create_data_key(key, )
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.STRING)
            async with Batch(self.db) as batch:
                cf = batch[self.cf]
                if not meta:
                    meta = KeyMeta()
                    meta.key_type = KeyTypeEnum.STRING.name
                    meta = meta.to_dict()
                    cf.put(meta_key, meta)
                cf.put(data_key, value)

    async def delete(self, key):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        data_key = codec.create_data_key(key, )
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key)
            if not meta:
                return
            if meta.key_type == KeyTypeEnum.STRING.name:
                async with Batch(self.db) as batch:
                    cf = batch[self.cf]
                    cf.delete(meta_key)
                    cf.delete(data_key)
            elif meta.key_type == KeyTypeEnum.LIST.name:
                await LinkedList.delete(codec, self.db, self.cf, key)

    async def llen(self, key) -> int:
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST)
            result = await LinkedList.size(meta)
            return result

    async def lpush(self, key, value):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST)
            if meta is None:
                await LinkedList.create(codec, self.db, self.cf, key, meta_key, value)
            else:
                await LinkedList.insert(codec, self.db, self.cf, key, meta, meta_key, 0, value)

    async def lpop(self, key) -> Optional[Any]:
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST, assert_exists=True)
            value = await LinkedList.remove(codec, self.db, self.cf, key, meta, meta_key, 0, True)
            return value

    async def rpush(self, key, value):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST)
            if meta is None:
                await LinkedList.create(codec, self.db, self.cf, key, meta_key, value)
            else:
                await LinkedList.insert(codec, self.db, self.cf, key, meta, meta_key, -1, value)

    async def rpop(self, key) -> Optional[Any]:
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST, assert_exists=True)
            value = await LinkedList.remove(codec, self.db, self.cf, key, meta, meta_key, -1, True)
            return value

    async def lindex(self, key, index) -> Any:
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST, assert_exists=True)
            result = await LinkedList.index_of(codec, self.cf, key, meta, index)
            return result

    async def lset(self, key, index, value):
        codec = ComplexCodec()
        meta_key = self.meta_key(codec, key)
        async with RedisCommand.LOCK.acquire(key):
            meta = await self.fetch_meta(self.cf, meta_key, KeyTypeEnum.LIST, assert_exists=True)
            await LinkedList.update_date(codec, self.cf, key, meta, index, value)


__all__ = ['RedisCommand', ]
