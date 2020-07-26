import abc
from typing import *
from .codec import *
from .rocks_db import *
from .error_type import *


class ColumnFamilyBase(abc.ABC):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        cf_dict = d.column_family_dict
        cf_name = cf_name or d.DEFAULT_COLUMN_FAMILY
        assert cf_name in cf_dict
        assert cf_dict[cf_name].is_loaded()
        self.d = d
        self.cf = cf_dict[cf_name]
        self.name = cf_name
        self.read_options = read_options

    @classmethod
    def to_name(cls, obj: object) -> str:
        if obj is None:
            return RocksDb.DEFAULT_COLUMN_FAMILY
        elif isinstance(obj, RColumnFamily):
            return obj.get_name()
        elif isinstance(obj, str):
            return obj
        elif isinstance(obj, ColumnFamilyBase):
            return obj.name
        else:
            raise ValueError(f'Unknown column family type: {type(obj)}')


class ColumnFamily(ColumnFamilyBase):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        super(ColumnFamily, self).__init__(cf_name=cf_name, d=d, read_options=read_options)

    def __str__(self):
        return f'<ColumnFamily {self.name}>'

    async def get(self, key: bytes, raise_exception=False) -> Optional[Any]:
        status = await self.d.get(key, column_family=self.cf, read_options=self.read_options)
        if status.ok():
            codec = Codec.find_codec(key, self.d.codec_list)
            value = codec.loads(status.result)
            return value
        else:
            if raise_exception:
                raise StatusError(status)
            else:
                return None

    async def put(self, key: bytes, value: object):
        assert not self.d.is_readonly
        codec = Codec.find_codec(key, self.d.codec_list)
        value = codec.dumps(value)
        status = await self.d.put(key, value, column_family=self.cf)
        if not status.ok():
            raise StatusError(status)

    async def delete(self, key: bytes):
        assert not self.d.is_readonly
        status = await self.d.delete(key, column_family=self.cf)
        if not status.ok():
            raise StatusError(status)


class SnapshotColumnFamily(ColumnFamilyBase):
    def __init__(self, cf_name: str, d: RocksDb, read_options=None):
        super(SnapshotColumnFamily, self).__init__(cf_name=cf_name, d=d, read_options=read_options)

    def __str__(self):
        return f'<SnapshotColumnFamily {self.name}>'

    async def get(self, key: bytes, raise_exception=False) -> Optional[object]:
        status = await self.d.get(key, column_family=self.cf, read_options=self.read_options)
        if status.ok():
            codec = Codec.find_codec(key, self.d.codec_list)
            value = codec.loads(status.result)
            return value
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

    def put(self, key: bytes, value: object):
        codec = Codec.find_codec(key, self.d.codec_list)
        value = codec.dumps(value)
        self.batch.put(key, value, self.cf)

    def delete(self, key: bytes):
        self.batch.delete_key(key, self.cf)


__all__ = [
    'ColumnFamilyBase',
    'ColumnFamily',
    'SnapshotColumnFamily',
    'BatchColumnFamily',
]