

class Codec:
    def __init__(self, prefix, loads=None, dumps=None, key_split=b':'):
        self.prefix = prefix
        self._loads = loads
        self._dumps = dumps
        self.key_split = key_split

    def loads(self, data):
        if self._loads:
            return self._loads(data)
        else:
            return data

    def dumps(self, data):
        if self._dumps:
            dump = self._dumps(data)
            return dump
        else:
            return data

    def prefix_match(self, key: bytes) -> bool:
        if self.prefix is None:
            return True
        return key.startswith(self.prefix)

    def create_key(self, *args):
        key = self.key_split.join([self.prefix] + list(args))
        return key

    @classmethod
    def find_codec(cls, key: bytes, codec_list):
        for codec in codec_list:
            if not codec.prefix_match(key):
                continue
            break
        else:
            raise ValueError(f'key {key} missing codec')
        return codec


__all__ = ['Codec', ]
