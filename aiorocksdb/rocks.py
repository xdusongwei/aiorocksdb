import aiorocksdb.wrap

class Status:
    def __init__(self, s):
        self.code = s.getCode()
        self.sub_code = s.getSubCode()

    def is_ok(self):
        return self.code == 0

    def __repr__(self):
        return f'<Status code:{self.code}, subCode:{self.sub_code}>'


class Rocks:
    @classmethod
    def default_column_family_list(cls):
        return ['default', ]

    def __init__(self):
        self._db = None
        self._column_family_list = None

    def open(self, path, create_if_missing=True, column_family_list=None):
        column_family_list = column_family_list or self.default_column_family_list()
        column_family_list = [aiorocksdb.wrap.RocksColumnFamily(s) for s in column_family_list]
        self._column_family_list = column_family_list
        self._db = aiorocksdb.wrap.RocksDb(path, create_if_missing)
        return Status(self._db.open(column_family_list))

    def close(self):
        for cf in self._column_family_list:
            self._db.destroyColumnFamily(cf)
        self._db.close()

    def create_snapshot(self):
        snapshot = aiorocksdb.wrap.RocksSnapshot()
        self._db.createSnapshot(snapshot)
        return snapshot

    def release_snapshot(self, snapshot):
        self._db.releaseSnapshot(snapshot)


__all__ = ['Rocks', ]
