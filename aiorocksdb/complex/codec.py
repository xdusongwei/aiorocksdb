import msgpack
from aiorocksdb.codec import *


class ComplexCodec(Codec):
    def __init__(self):
        super(ComplexCodec, self).__init__(b'__redis:complex', loads=msgpack.loads, dumps=msgpack.dumps)

    @classmethod
    def create_data_key(cls, *args):
        key = b':'.join(args)
        assert not key.startswith(b'__redis')
        return key


__all__ = ['ComplexCodec', ]
