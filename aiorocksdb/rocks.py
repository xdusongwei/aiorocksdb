import aiorocksdb.wrap


class Rocks:
    DEFAULT_COLUMN_FAMILY_LIST = [aiorocksdb.wrap.ColumnFamily('default')]
    def __init__(self):
        self._db = None
        self._column_family_list = None

    def open(self, path, create_if_missing=True, column_family_list=None):
        column_family_list = column_family_list or self.DEFAULT_COLUMN_FAMILY_LIST
        self._column_family_list = column_family_list
        self._db = aiorocksdb.wrap.RocksDb(path, create_if_missing)
        self._db.open(column_family_list)

    def close(self):
        for cf in self._column_family_list:
            self._db.destroyColumnFamily(cf)
        self._db.close()


__all__ = ['Rocks', ]
