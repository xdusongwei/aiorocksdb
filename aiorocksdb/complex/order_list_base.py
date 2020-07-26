import abc
import random
from typing import *
from aiorocksdb.meta import *
from aiorocksdb.complex.node_base import *


class OrderedListBase(abc.ABC, NodeBase):
    def __init__(self, codec, cf, meta: KeyMeta):
        super(OrderedListBase, self).__init__()
        self._node_cache = dict()
        self.codec = codec
        self.cf = cf
        self.meta = meta

    async def fetch_node_meta(self, key, t: Type[MetaBase]) -> MetaBase:
        if key in self._node_cache:
            return self._node_cache[key]
        cf = self.cf
        meta = await NodeBase.fetch_node_meta(cf, key, t)
        self._node_cache[key] = meta
        return meta

    @classmethod
    def coin(cls):
        return random.randint(0, 1)

    def generate_mask(self) -> int:
        height = self.meta.height()
        mask_length = 1
        for i in range(height - 1):
            coin = self.coin()
            if not coin:
                break
            mask_length += 1
        return mask_length

    @classmethod
    def list_get(cls, lst, index: int, default=None):
        if index < 0 or index >= len(lst):
            return default
        return lst[index]

    def initialize_border_nodes(self, field):
        left_nodes = dict()
        right_nodes = dict()

        if field > self.meta.seq:
            for level, seq in enumerate(self.meta.tail_key):
                left_nodes[level] = self.list_get(self.meta.tail_key, level)
            for level, seq in enumerate(self.meta.tail_key):
                right_nodes[level] = None
        else:
            for level, seq in enumerate(self.meta.head_key):
                left_nodes[level] = None
            for level, seq in enumerate(self.meta.tail_key):
                right_nodes[level] = self.list_get(self.meta.head_key, level)

        return left_nodes, right_nodes

    async def search(self, key, field, left_nodes: dict, right_nodes: dict):
        current_field = None
        next_stack: List[Any] = self.meta.head_key
        height = len(self.meta.head_key)
        for i in range(height):
            level = height - 1 - i
            while self.list_get(next_stack, level) and self.list_get(next_stack, level) <= field:
                current_field = self.list_get(next_stack, level)
                node_key = NodeBase.node_key(self.codec, key, current_field)
                node = await self.fetch_node_meta(node_key, SkipListNode)
                next_stack = node.next
            node_key = NodeBase.node_key(self.codec, key, current_field)
            node: SkipListNode = await self.fetch_node_meta(node_key, SkipListNode)
            if node:
                left_nodes[level] = node.seq
                right_nodes[level] = self.list_get(node.next, level)
        return current_field

    async def insert(self, key, field, value=b''):
        cache_nodes = self.cache_nodes

        left_nodes, right_nodes = self.initialize_border_nodes(field)

        current_field = await self.search(key, field, left_nodes, right_nodes)

        if current_field == field:
            batch = self.batch
            codec = self.codec
            # save data node
        else:
            new_node = self.new_node(score)
            node_height = self.generate_mask()
            for level in range(node_height):
                left_seq = left_nodes.get(level)
                if not left_seq:
                    self.list_set(self.key_meta.head_key, level, score)
                else:
                    node = await self.cache_or_get(cache_nodes, left_seq)
                    self.list_set(node.next, level, score)
                    self.list_set(new_node.prev, level, left_seq)
                    assert node.seq < new_node.seq
                right_seq = right_nodes.get(level)
                if not right_seq:
                    self.list_set(self.key_meta.tail_key, level, score)
                else:
                    node = await self.cache_or_get(cache_nodes, right_seq)
                    self.list_set(node.prev, level, score)
                    self.list_set(new_node.next, level, right_seq)
                    assert node.seq > new_node.seq
            self.key_meta.length += 1
            self.key_meta.seq = max(score, self.key_meta.seq)
            batch = self.batch
            await self.changed_node_batch(batch)
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, new_node.seq), value)
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, new_node.seq[1]),
                          pickle.dumps(new_node.seq))
            new_node.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, new_node.seq), batch)
            self.key_meta.save_meta(self.key, batch)
        return score


__all__ = ['OrderedListBase', ]
