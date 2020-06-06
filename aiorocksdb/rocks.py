import aiorocksdb.rocks_wrap


class Status:
    def __init__(self, s):
        self.code = s.getCode()
        self.sub_code = s.getSubCode()

    def is_ok(self):
        return self.code == 0

    def __repr__(self):
        return f'<Status code:{self.code}, subCode:{self.sub_code}>'


class Transaction:
    def get_for_update(self, key):
        pass

    def get(self, key):
        pass

    def put(self, key, value):
        pass

    def delete(self, key):
        pass

    def merge(self, key, value):
        pass

    def commit(self):
        pass

    def set_snapshot(self):
        pass

    def set_save_point(self):
        pass

    def rollback_to_save_point(self):
        pass


class Rocks:
    @classmethod
    def default_column_family_list(cls):
        return ['default', ]

    def __init__(self):
        self._db = None
        self._column_family_list = None

    def open_db(self, path, create_if_missing=True, column_family_list=None):
        column_family_list = column_family_list or self.default_column_family_list()
        column_family_list = [aiorocksdb.rocks_wrap.RocksColumnFamily(s) for s in column_family_list]
        self._column_family_list = column_family_list
        self._db = aiorocksdb.rocks_wrap.Rocks(path, create_if_missing)
        return Status(self._db.open_db(column_family_list))

    def open_transaction_db(self, path, create_if_missing=True, column_family_list=None):
        column_family_list = column_family_list or self.default_column_family_list()
        column_family_list = [aiorocksdb.rocks_wrap.RocksColumnFamily(s) for s in column_family_list]
        self._column_family_list = column_family_list
        self._db = aiorocksdb.rocks_wrap.Rocks(path, create_if_missing)
        return Status(self._db.open_transaction_db(column_family_list))

    def open_optimistic_transaction_db(self, path, create_if_missing=True, column_family_list=None):
        column_family_list = column_family_list or self.default_column_family_list()
        column_family_list = [aiorocksdb.rocks_wrap.RocksColumnFamily(s) for s in column_family_list]
        self._column_family_list = column_family_list
        self._db = aiorocksdb.rocks_wrap.Rocks(path, create_if_missing)
        return Status(self._db.open_optimistic_transaction_db(column_family_list))

    def close(self):
        for cf in self._column_family_list:
            self._db.destroyColumnFamily(cf)
        self._db.close()

    def create_snapshot(self):
        snapshot = aiorocksdb.rocks_wrap.RocksSnapshot()
        self._db.createSnapshot(snapshot)
        return snapshot

    def release_snapshot(self, snapshot):
        self._db.releaseSnapshot(snapshot)


__all__ = ['Rocks', ]
