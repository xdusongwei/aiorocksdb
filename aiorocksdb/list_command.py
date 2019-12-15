from aiorocksdb.meta import *
from aiorocksdb.batch import BatchContext
from aiorocksdb.const import *


class ListCommand:
    def __init__(self, key: str, key_meta: KeyMeta, db):
        self.key = key
        self.key_meta = key_meta
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.key = None
        self.key_meta = None
        self.db = None

    def new_node(self):
        seq = self.key_meta.increase_seq()
        node = ListNode()
        node.seq = seq
        node.next = None
        node.prev = None
        return node

    async def find_node_by_seq(self, seq):
        node_key = f'{META_KEY_PREFIX}{self.key}:{seq}'
        node = await ListNode.find_meta(node_key, self.db)
        return node

    @classmethod
    def fast_direction(cls, direction: bool, length: int, index: int):
        middle_index = length // 2
        if direction and index > middle_index:
            index = length - index - 1
            direction = not direction
        elif not direction and index > middle_index:
            index = length - index - 1
            direction = not direction
        return direction, index

    async def remove_by_index(self, index: int):
        next_direction = True
        if index < 0:
            index = abs(index)
            if index > self.key_meta.length + 1:
                raise ValueError(f'index -{index} overflow, list length is {self.key_meta.length}')
            index -= 1
            next_direction = False
        else:
            if index > self.key_meta.length - 1:
                raise ValueError(f'index {index} overflow, list length is {self.key_meta.length}')
        next_direction, index = self.fast_direction(next_direction, self.key_meta.length, index)
        node_seq = self.key_meta.head_key if next_direction else self.key_meta.tail_key
        while index:
            node = await self.find_node_by_seq(node_seq)
            node_seq = node.next if next_direction else node.prev
            index -= 1
        node = await self.find_node_by_seq(node_seq)
        node_left_seq = node.prev
        node_right_seq = node.next
        node_left = None
        node_right = None
        if node_left_seq:
            node_left = await self.find_node_by_seq(node_left_seq)
            node_left.next = node_right_seq
        else:
            self.key_meta.head_key = node.next
        if node_right_seq:
            node_right = await self.find_node_by_seq(node_right_seq)
            node_right.prev = node_left_seq
        else:
            self.key_meta.tail_key = node.prev
        self.key_meta.length -= 1
        async with BatchContext(self.db) as batch:
            batch.delete(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, node.seq))
            batch.delete(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node.seq))
            if node_left:
                node_left.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node_left.seq), batch)
            if node_right:
                node_right.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node_right.seq), batch)
            self.key_meta.save_meta(self.key, batch)

    async def index_of(self, index: int):
        next_direction = True
        if index < 0:
            index = abs(index)
            if index > self.key_meta.length:
                raise ValueError(f'index -{index} overflow, list length is {self.key_meta.length}')
            index -= 1
            next_direction = False
        else:
            if index > self.key_meta.length - 1:
                raise ValueError(f'index {index} overflow, list length is {self.key_meta.length}')
        next_direction, index = self.fast_direction(next_direction, self.key_meta.length, index)
        node_seq = self.key_meta.head_key if next_direction else self.key_meta.tail_key
        while index:
            node = await self.find_node_by_seq(node_seq)
            node_seq = node.next if next_direction else node.prev
            index -= 1
        data_key = KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, node_seq)
        value = await self.db.get(data_key)
        return value

    async def insert(self, index: int, value: bytes):
        next_direction = True
        if index < 0:
            index = abs(index)
            if index > self.key_meta.length + 1:
                raise ValueError(f'index -{index} overflow, list length is {self.key_meta.length}')
            index -= 1
            next_direction = False
        else:
            if index > self.key_meta.length:
                raise ValueError(f'index {index} overflow, list length is {self.key_meta.length}')
        next_direction, index = self.fast_direction(next_direction, self.key_meta.length, index)
        node_left_seq = self.key_meta.tail_key if not next_direction else None
        node_right_seq = self.key_meta.head_key if next_direction else None
        while index:
            if next_direction:
                node = await self.find_node_by_seq(node_right_seq)
                node_left_seq = node_right_seq
                node_right_seq = node.next
            else:
                node = await self.find_node_by_seq(node_left_seq)
                node_right_seq = node_left_seq
                node_left_seq = node.prev
            index -= 1
        node_left = None
        node_right = None
        if node_left_seq:
            node_left = await self.find_node_by_seq(node_left_seq)
        if node_right_seq:
            node_right = await self.find_node_by_seq(node_right_seq)
        new_node = self.new_node()
        if node_left:
            node_left.next = new_node.seq
        if node_right:
            node_right.prev = new_node.seq
        new_node.prev = node_left_seq
        new_node.next = node_right_seq
        if not node_left_seq:
            self.key_meta.head_key = new_node.seq
        if not node_right_seq:
            self.key_meta.tail_key = new_node.seq
        self.key_meta.length += 1
        async with BatchContext(self.db) as batch:
            batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, new_node.seq), value)
            new_node.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, new_node.seq), batch)
            if node_left:
                node_left.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node_left.seq), batch)
            if node_right:
                node_right.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node_right.seq), batch)
            self.key_meta.save_meta(self.key, batch)

    async def set(self, index: int, value: bytes):
        next_direction = True
        if index < 0:
            index = abs(index)
            if index > self.key_meta.length:
                raise ValueError(f'index -{index} overflow, list length is {self.key_meta.length}')
            index -= 1
            next_direction = False
        else:
            if index > self.key_meta.length - 1:
                raise ValueError(f'index {index} overflow, list length is {self.key_meta.length}')
        next_direction, index = self.fast_direction(next_direction, self.key_meta.length, index)
        node_seq = self.key_meta.head_key if next_direction else self.key_meta.tail_key
        while index:
            node = await self.find_node_by_seq(node_seq)
            node_seq = node.next if next_direction else node.prev
            index -= 1
        async with BatchContext(self.db) as batch:
            data_key = KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, node_seq)
            batch.put(data_key, value)

    async def deconstruct(self):
        node_seq = self.key_meta.head_key
        seq_list = list()
        while node_seq:
            node = await self.find_node_by_seq(node_seq)
            seq_list.append(node.seq)
            node_seq = node.next
        async with BatchContext(self.db) as batch:
            for seq in seq_list:
                batch.delete(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, seq))
                batch.delete(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, seq))
            batch.delete(self.key)


__all__ = ['ListCommand', ]
