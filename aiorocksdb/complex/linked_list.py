from typing import *
from aiorocksdb.meta import *
from aiorocksdb.batch import *
from aiorocksdb.iterator import *
from aiorocksdb.complex.node_base import *


class LinkedList(NodeBase):
    @classmethod
    async def _find_node_by_steps(cls, codec, cf, key, ascending: bool, steps: int, begin_seq: int):
        node_seq = begin_seq
        while True:
            node_key = cls.node_key(codec, key, node_seq)
            node_meta = await cls.fetch_node_meta(cf, node_key, ListNode)
            if steps <= 0:
                break
            else:
                node_seq = node_meta.next if ascending else node_meta.prev
                steps -= 1
        return node_meta

    @classmethod
    def format_index(cls, length: int, index: int, positive_offset=0, negative_offset=0):
        """
        reformat index
        index is [0, 1, 2, 3, ..., n]
        result is True, [0, 1, 2, 3, ..., n]

        index is [-1, -2, -3, ..., -n]
        result is False, [0, 1, 2, 3, ..., n]
        """
        ascending = True
        if index < 0:
            index = abs(index)
            if index > length + negative_offset:
                raise ValueError(f'index -{index} overflow, list length is {length}')
            index -= 1
            ascending = False
        else:
            if index > length + positive_offset:
                raise ValueError(f'index {index} overflow, list length is {length}')
        return ascending, index

    @classmethod
    def fast_direction(cls, ascending: bool, length: int, index: int):
        """
        choose the shortest scan direction
        """
        middle_index = length // 2
        if ascending and index > middle_index:
            index = length - index - 1
            ascending = not ascending
        elif not ascending and index > middle_index:
            index = length - index - 1
            ascending = not ascending
        return ascending, index

    @classmethod
    async def create(cls, codec, db, cf, key, meta_key, value):
        meta = KeyMeta()
        meta.key_type = KeyTypeEnum.LIST.name
        meta.length = 1
        meta.seq = 0

        new_node = ListNode()
        new_node.seq = meta.increase_seq()

        node_key = cls.node_key(codec, key, new_node.seq)
        data_key = cls.data_key(codec, key, new_node.seq)

        meta.head_seq = meta.tail_seq = new_node.seq
        meta = meta.to_dict()
        node = new_node.to_dict()

        async with Batch(db) as batch:
            cf = batch[cf]
            cf.put(meta_key, meta)
            cf.put(node_key, node)
            cf.put(data_key, value)

    @classmethod
    async def delete(cls, codec, db, cf, key):
        keys = list()
        meta_key = codec.create_key(b'meta', key)
        node_key_prefix = cls.node_key(codec, key) + codec.key_split
        data_key_prefix = cls.data_key(codec, key) + codec.key_split
        keys.append(meta_key)
        async with Iterator.prefix(cf, node_key_prefix) as iter:
            async for key, _ in iter:
                keys.append(key)
        async with Iterator.prefix(cf, data_key_prefix) as iter:
            async for key, _ in iter:
                keys.append(key)
        async with Batch(db) as batch:
            cf = batch[cf]
            for key in keys:
                cf.delete(key)

    @classmethod
    async def insert(cls, codec, db, cf, key, meta, meta_key, index, value):
        ascending, index = cls.format_index(meta.length, index, negative_offset=1)
        ascending, steps = cls.fast_direction(ascending, meta.length, index)
        begin_seq = meta.head_seq if ascending else meta.tail_seq
        node_meta = await cls._find_node_by_steps(codec, cf, key, ascending, steps, begin_seq)
        left_node_meta = None
        right_node_meta = None
        if ascending:
            if node_meta.prev is not None:
                left_node_key = cls.node_key(codec, key, node_meta.prev)
                left_node_meta = await cls.fetch_node_meta(cf, left_node_key, ListNode)
            right_node_meta = node_meta
        else:
            left_node_meta = node_meta
            if node_meta.next is not None:
                right_node_key = cls.node_key(codec, key, node_meta.next)
                right_node_meta = await cls.fetch_node_meta(cf, right_node_key, ListNode)
        new_node = ListNode()
        new_node.seq = meta.increase_seq()
        meta.length += 1
        if left_node_meta:
            new_node.prev = left_node_meta.seq
            left_node_meta.next = new_node.seq
        else:
            meta.head_seq = new_node.seq
        if right_node_meta:
            new_node.next = right_node_meta.seq
            right_node_meta.prev = new_node.seq
        else:
            meta.tail_seq = new_node.seq
        node_key = cls.node_key(codec, key, new_node.seq)
        data_key = cls.data_key(codec, key, new_node.seq)
        meta = meta.to_dict()
        node = new_node.to_dict()
        async with Batch(db) as batch:
            cf = batch[cf]
            cf.put(meta_key, meta)
            cf.put(node_key, node)
            cf.put(data_key, value)
            if left_node_meta:
                node_key = cls.node_key(codec, key, left_node_meta.seq)
                node = left_node_meta.to_dict()
                cf.put(node_key, node)
            if right_node_meta:
                node_key = cls.node_key(codec, key, right_node_meta.seq)
                node = right_node_meta.to_dict()
                cf.put(node_key, node)

    @classmethod
    async def remove(cls, codec, db, cf, key, meta, meta_key, index, return_value=False):
        remove_meta = False
        ascending, index = cls.format_index(meta.length, index, positive_offset=-1, negative_offset=1)
        ascending, steps = cls.fast_direction(ascending, meta.length, index)
        begin_seq = meta.head_seq if ascending else meta.tail_seq
        node_meta = await cls._find_node_by_steps(codec, cf, key, ascending, steps, begin_seq)
        left_node_meta = None
        right_node_meta = None
        if node_meta.prev is not None:
            left_node_key = cls.node_key(codec, key, node_meta.prev)
            left_node_meta = await cls.fetch_node_meta(cf, left_node_key, ListNode)
        if node_meta.next is not None:
            right_node_key = cls.node_key(codec, key, node_meta.next)
            right_node_meta = await cls.fetch_node_meta(cf, right_node_key, ListNode)
        if left_node_meta and right_node_meta:
            left_node_meta.next = right_node_meta.seq
            right_node_meta.prev = left_node_meta.seq
        elif left_node_meta:
            left_node_meta.next = None
        elif right_node_meta:
            right_node_meta.prev = None
        else:
            remove_meta = True
        node_key = cls.node_key(codec, key, node_meta.seq)
        data_key = cls.data_key(codec, key, node_meta.seq)
        meta.length -= 1
        meta = meta.to_dict()
        value = None
        if return_value:
            value = await cf.get(data_key)
        async with Batch(db) as batch:
            cf = batch[cf]
            cf.delete(node_key)
            cf.delete(data_key)
            if left_node_meta:
                node_key = cls.node_key(codec, key, left_node_meta.seq)
                node = left_node_meta.to_dict()
                cf.put(node_key, node)
            if right_node_meta:
                node_key = cls.node_key(codec, key, right_node_meta.seq)
                node = right_node_meta.to_dict()
                cf.put(node_key, node)
            if remove_meta:
                cf.delete(meta_key)
            else:
                cf.put(meta_key, meta)
        return value

    @classmethod
    async def size(cls, meta: KeyMeta) -> int:
        if meta is None:
            return 0
        else:
            return meta.length

    @classmethod
    async def index_of(cls, codec, cf, key, meta, index):
        ascending, index = cls.format_index(meta.length, index, positive_offset=-1)
        ascending, steps = cls.fast_direction(ascending, meta.length, index)
        begin_seq = meta.head_seq if ascending else meta.tail_seq
        node_meta = await cls._find_node_by_steps(codec, cf, key, ascending, steps, begin_seq)
        data_key = cls.data_key(codec, key, node_meta.seq)
        value = await cf.get(data_key)
        return value

    @classmethod
    async def update_date(cls, codec, cf, key, meta, index, value):
        ascending, index = cls.format_index(meta.length, index, positive_offset=-1)
        ascending, steps = cls.fast_direction(ascending, meta.length, index)
        begin_seq = meta.head_seq if ascending else meta.tail_seq
        node_meta = await cls._find_node_by_steps(codec, cf, key, ascending, steps, begin_seq)
        data_key = cls.data_key(codec, key, node_meta.seq)
        await cf.put(data_key, value)


__all__ = ['LinkedList', ]
