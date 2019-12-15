import random
import pickle
from aiorocksdb.meta import *
from aiorocksdb.const import *
from aiorocksdb.batch import *


class SkipListCommand:
    def __init__(self, key: str, key_meta: KeyMeta, db):
        self.key = key
        self.key_meta = key_meta
        self.db = db
        self.cache_nodes = dict()
        self.changed_nodes = set()
        self.batch = Batch()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.db.execute(self.batch)
        self.key = None
        self.key_meta = None
        self.db = None

    def list_get(self, lst, index: int, default=None):
        if index < 0 or index >= len(lst):
            return default
        return lst[index]

    def list_set(self, lst, index: int, v):
        while index >= len(lst):
            lst.append(None)
        lst[index] = v
        while len(lst):
            if lst[-1] is not None:
                break
            lst.pop(-1)

    def new_node(self, seq=None):
        seq = seq or self.key_meta.increase_seq()
        node = SkipListNode()
        node.seq = seq
        node.next = list()
        node.prev = list()
        return node

    async def find_node_by_seq(self, seq):
        node_key = KeyMeta.build_key_path(META_KEY_PREFIX, self.key, seq)
        node = await SkipListNode.find_meta(node_key, self.db)
        return node

    @classmethod
    def coin(cls):
        return random.randint(0, 1)

    def generate_mask(self) -> int:
        height = self.key_meta.height()
        mask_length = 1
        for i in range(height - 1):
            coin = self.coin()
            if not coin:
                break
            mask_length += 1
        return mask_length

    async def cache_or_get(self, cache_dict, seq):
        if seq not in cache_dict:
            node = await self.find_node_by_seq(seq)
            cache_dict[seq] = node
        node = cache_dict[seq]
        return node

    async def find_value_by_score(self, score):
        if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
            value = await self.db.get(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, score))
        elif self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_SET:
            value = await self.db.get(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, score))
        elif self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
            value = await self.db.get(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, score))
            if value:
                value = pickle.loads(value)
        else:
            raise ValueError(f'Unknown key type {self.key_meta.key_type}')
        return value

    async def all_score(self):
        score_list = list()
        seq = self.list_get(self.key_meta.head_key, 0)
        while seq:
            node = await self.find_node_by_seq(seq)
            score_list.append(node.seq)
            seq = self.list_get(node.next, 0)
        return score_list

    async def changed_node_batch(self, batch):
        for seq in self.changed_nodes:
            changed_node = await self.cache_or_get(self.cache_nodes, seq)
            changed_node.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, seq), batch)

    async def remove_by_score(self, score):
        cache_nodes = self.cache_nodes
        changed_seq = self.changed_nodes
        node = await self.cache_or_get(cache_nodes, score)
        if not node:
            return False
        for level, left_seq in enumerate(node.prev):
            node_left = await self.cache_or_get(cache_nodes, left_seq)
            self.list_set(node_left.next, level, self.list_get(node.next, level))
            changed_seq.add(node_left.seq)
        for level, right_seq in enumerate(node.next):
            node_right = await self.cache_or_get(cache_nodes, right_seq)
            self.list_set(node_right.prev, level, self.list_get(node.prev, level))
            changed_seq.add(node_right.seq)
        for level, seq in enumerate(self.key_meta.head_key.copy()):
            if seq != score:
                continue
            self.list_set(self.key_meta.head_key, level, self.list_get(node.next, level))
        for level, seq in enumerate(self.key_meta.tail_key.copy()):
            if seq != score:
                continue
            self.list_set(self.key_meta.tail_key, level, self.list_get(node.prev, level))
        self.key_meta.length -= 1
        batch = self.batch
        await self.changed_node_batch(batch)
        if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
            batch.delete(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, node.seq))
        if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
            batch.delete(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, node.seq[1]))
        batch.delete(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, node.seq))
        self.key_meta.save_meta(self.key, batch)
        return True

    async def insert(self, score, value: bytes):
        left_nodes = dict()
        right_nodes = dict()
        cache_nodes = self.cache_nodes
        changed_seq = self.changed_nodes
        if score > self.key_meta.seq:
            for level, seq in enumerate(self.key_meta.tail_key):
                left_nodes[level] = self.list_get(self.key_meta.tail_key, level)
            for level, seq in enumerate(self.key_meta.tail_key):
                right_nodes[level] = None
        else:
            for level, seq in enumerate(self.key_meta.head_key):
                left_nodes[level] = None
            for level, seq in enumerate(self.key_meta.tail_key):
                right_nodes[level] = self.list_get(self.key_meta.head_key, level)
        current_score = None
        next_stack = self.key_meta.head_key
        height = len(self.key_meta.head_key)
        for level in range(height):
            level = height - 1 - level
            while self.list_get(next_stack, level) and self.list_get(next_stack, level) <= score:
                current_score = self.list_get(next_stack, level)
                node = await self.cache_or_get(cache_nodes, current_score)
                next_stack = node.next
            node = await self.cache_or_get(cache_nodes, current_score)
            if node:
                left_nodes[level] = node.seq
                right_nodes[level] = self.list_get(node.next, level)
        if current_score == score:
            batch = self.batch
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, current_score), value)
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, current_score[1]), pickle.dumps(current_score))
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
            for v in left_nodes.values():
                if not v:
                    continue
                changed_seq.add(v)
            for v in right_nodes.values():
                if not v:
                    continue
                changed_seq.add(v)
            batch = self.batch
            await self.changed_node_batch(batch)
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, new_node.seq), value)
            if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
                batch.put(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, new_node.seq[1]), pickle.dumps(new_node.seq))
            new_node.save_meta(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, new_node.seq), batch)
            self.key_meta.save_meta(self.key, batch)
        return score

    async def deconstruct(self):
        node_seq = self.list_get(self.key_meta.head_key, 0)
        seq_list = list()
        while node_seq:
            node = await self.find_node_by_seq(node_seq)
            seq_list.append(node.seq)
            node_seq = self.list_get(node.next, 0)
        async with self.db.batch_context() as batch:
            for seq in seq_list:
                batch.delete(KeyMeta.build_key_path(DATA_KEY_PREFIX, self.key, seq))
                if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST:
                    batch.delete(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, seq))
                if self.key_meta.key_type == KeyTypeEnum.ORDER_LIST_ZSET:
                    batch.delete(KeyMeta.build_key_path(META_KEY_PREFIX, self.key, seq[1]))
            batch.delete(self.key)


__all__ = ['SkipListCommand', ]
