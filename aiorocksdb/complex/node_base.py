import abc
import msgpack
from typing import *
from aiorocksdb.meta import *


class NodeBase(abc.ABC):
    @classmethod
    async def fetch_meta(cls, cf, key, key_type: KeyTypeEnum = None, assert_exists=False) -> Optional[KeyMeta]:
        meta = await cf.get(key)
        if meta:
            meta = KeyMeta.from_dict(meta)
            if key_type is not None:
                assert meta.key_type == key_type.name
        if assert_exists:
            assert meta is not None
        return meta

    @classmethod
    async def fetch_node_meta(cls, cf, key, t: Type[MetaBase]) -> MetaBase:
        meta = await cf.get(key)
        if meta:
            meta = t.from_dict(meta)
        return meta

    @classmethod
    def meta_key(cls, codec, key):
        return codec.create_key(b'meta', key)

    @classmethod
    def node_key(cls, codec, key: bytes, node_name: Any = None):
        if node_name is None:
            node_key = codec.create_key(b'node', key, )
        else:
            if isinstance(node_name, list) or isinstance(node_name, tuple):
                dump_name = msgpack.dumps(node_name)
                node_key = codec.create_key(b'node', key, dump_name)
            else:
                node_key = codec.create_key(b'node', key, bytes(str(node_name), 'utf8'))
        return node_key

    @classmethod
    def data_key(cls, codec, key: bytes, node_name: Any = None):
        if node_name is None:
            data_key = codec.create_data_key(key)
        else:
            data_key = codec.create_data_key(key, bytes(str(node_name), 'utf8'))
        return data_key


__all__ = ['NodeBase', ]
