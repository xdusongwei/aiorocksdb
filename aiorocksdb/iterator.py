from typing import *
from .error_type import *
from .rocks_db import *
from .codec import *
from .column_family import *


class Iterator:
    def __init__(
            self,
            c: ColumnFamilyBase,
            prefix=None,
            start: bytes = None,
            stop: bytes = None,
            desc=False,
            seek_to_prev=False,
            read_options=None,
    ):
        self.desc = desc
        self.prefix = prefix
        self.start = start
        self.stop = stop
        self.c = c
        self.options = read_options
        self.iterator: RocksDbIterator = None
        self.seek_to_prev = seek_to_prev
        if start is not None and stop is not None:
            assert start <= stop

    @classmethod
    def prefix(cls, c: ColumnFamilyBase, prefix, read_options=None):
        return cls(c, prefix=prefix, read_options=read_options)

    @classmethod
    def range(cls, c: ColumnFamilyBase, start: bytes = None, stop: bytes = None, desc=False, seek_to_prev=False, read_options=None):
        return cls(c, start=start, stop=stop, desc=desc, seek_to_prev=seek_to_prev, read_options=read_options)

    async def seek(self, key: bytes):
        if self.seek_to_prev:
            await self.iterator.seek_for_prev(key)
        else:
            await self.iterator.seek(key)

    async def _build_iterator(self):
        if self.iterator:
            return
        db = self.c.d
        cf = self.c.cf
        self.iterator = await db.create_iterator(self.options, cf)
        if self.prefix is not None:
            await self.iterator.seek(self.prefix)
            self.desc = False
            self.start = None
            self.stop = None
        elif self.start is None and self.stop is None:
            if self.desc:
                await self.iterator.seek_to_last()
            else:
                await self.iterator.seek_to_first()
        elif self.start is None:
            if self.desc:
                await self.seek(self.stop)
            else:
                await self.iterator.seek_to_first()
        elif self.stop is None:
            if self.desc:
                await self.iterator.seek_to_last()
            else:
                await self.seek(self.start)
        else:
            if self.desc:
                await self.seek(self.stop)
            else:
                await self.seek(self.start)

    async def open(self):
        await self._build_iterator()

    async def close(self):
        await self.iterator.close()
        self.iterator = None

    async def __aenter__(self):
        await self._build_iterator()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self) -> Tuple[bytes, bytes]:
        valid = await self.iterator.valid()
        if not valid:
            status = await self.iterator.status()
            if not status.ok():
                raise StatusError(status)
            else:
                raise StopAsyncIteration
        key = await self.iterator.key()
        if self.prefix is not None:
            if not key.startswith(self.prefix):
                raise StopAsyncIteration
        if self.desc and self.start is not None:
            if key < self.start:
                raise StopAsyncIteration
        if not self.desc and self.stop is not None:
            if key > self.stop:
                raise StopAsyncIteration
        value = await self.iterator.value()
        if self.desc:
            await self.iterator.prev()
        else:
            await self.iterator.next()
        d = self.c.d
        codec = Codec.find_codec(key, d.codec_list)
        value = codec.loads(value)
        return key, value,


__all__ = ['Iterator', ]
