import enum
import math
import pickle
from abc import ABC
from urllib.parse import unquote, quote
from aiorocksdb.const import *


class KeyTypeEnum(enum.Enum):
    STRING = enum.auto()
    LIST = enum.auto()
    ORDER_LIST = enum.auto()
    ORDER_LIST_SET = enum.auto()
    ORDER_LIST_ZSET = enum.auto()


class MetaBase(ABC):
    @classmethod
    async def find_meta(cls, key: str, db):
        meta_key = key
        binary = await db.get(meta_key)
        if not binary:
            return None
        meta = pickle.loads(binary)
        return meta

    def save_meta(self, key: str, batch):
        meta_key = key
        binary = pickle.dumps(self)
        batch.put(meta_key, binary)


class KeyMeta(MetaBase):
    key_type: KeyTypeEnum = None
    ttl: int = -1
    length: int = 0
    head_key: str = None
    tail_key: str = None
    seq: int = 0

    def height(self):
        height = int(math.log2(max(self.length, 1)))
        height = max(ORDER_LIST_HEIGHT_MIN, height)
        return height

    def increase_seq(self):
        seq = self.seq = self.seq + 1
        return seq

    @classmethod
    def build_key_path(cls, prefix, key, seq):
        seq = str(seq)
        return f'{prefix}{quote(key)}:{quote(seq)}'


class ListNode(MetaBase):
    prev: str = None
    next: str = None
    seq: int = None


class SkipListNode(MetaBase):
    prev: list = list()
    next: list = list()
    seq: int = None

    def __str__(self):
        return f'<SkipListNode: {self.seq} prev: {self.prev} next: {self.next}>'


__all__ = ['KeyTypeEnum', 'KeyMeta', 'ListNode', 'SkipListNode', ]
