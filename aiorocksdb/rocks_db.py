from typing import *
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from aiorocksdb.db_type import *
from aiorocksdb.db_native import *
from aiorocksdb.db_api import *


import faulthandler
faulthandler.enable()


class AsyncCallMixin:
    def _aio_call(self, fn: Callable, *args, **kwargs):
        return self.loop.run_in_executor(
            self.thread, partial(fn, *args, **kwargs)
        )

    async def _sio_call(self, fn: Callable, *args, **kwargs):
        return fn(*args, **kwargs)

    def __init__(self):
        self.thread = ThreadPoolExecutor()
        self.aio_call = self._aio_call
        self.loop = asyncio.get_event_loop()

    def close_executor(self):
        if not self.thread:
            return
        self.thread.shutdown(wait=False)
        self.aio_call = None
        self.thread = None


class RocksDbTransaction:
    def __init__(self, aio_call, transaction: RTransaction, db):
        self.aio_call = aio_call
        self.transaction = transaction
        self.db = db

    async def put(self, key: bytes, value: bytes, column_family: RColumnFamily = None) -> StatusT:
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)
        column_family = column_family or self.db.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self.transaction.put, column_family, key, value)
        return status

    async def delete(self, key: bytes, column_family: RColumnFamily = None) -> StatusT:
        assert isinstance(key, bytes)
        column_family = column_family or self.db.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self.transaction.delete_key, column_family, key)
        return status

    async def commit(self) -> StatusT:
        status = await self.aio_call(self.transaction.commit)
        return status

    async def set_save_point(self):
        await self.aio_call(self.transaction.set_save_point)

    async def rollback(self) -> StatusT:
        status = await self.aio_call(self.transaction.rollback)
        return status

    async def clear_snapshot(self):
        await self.aio_call(self.transaction.clear_snapshot)

    async def prepare(self) -> StatusT:
        status = await self.aio_call(self.transaction.prepare)
        return status

    async def rollback_to_save_point(self) -> StatusT:
        status = await self.aio_call(self.transaction.rollback_to_save_point)
        return status

    async def pop_save_point(self) -> StatusT:
        status = await self.aio_call(self.transaction.pop_save_point)
        return status

    async def multi_get(self, pairs: List[Tuple[bytes, RColumnFamilyT]], read_options: ReadOptionsT = None) \
            -> List[StatusT[bytes]]:
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        keys, cf_list = list(zip(*pairs))
        keys = list(keys)
        cf_list = list(cf_list)
        complex_status: ComplexStatusT = await self.aio_call(self.transaction.multi_get, read_options, cf_list, keys)
        status_list = complex_status.status_list
        for idx in range(len(status_list)):
            status_list[idx].result = complex_status.value_list[idx]
        return status_list

    async def get_for_update(self, key: bytes, read_options: ReadOptionsT = None, column_family: RColumnFamilyT = None) \
            -> StatusT[bytes]:
        assert isinstance(key, bytes)
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        complex_status: ComplexStatusT = await self.aio_call(self.transaction.get_for_update, read_options, column_family, key)
        status = complex_status.status
        status.result = complex_status.value
        return status

    async def set_snapshot(self):
        await self.aio_call(self.transaction.set_snapshot)


class RocksDbIterator:
    def __init__(self, aio_call, iterator: RIterator):
        self.aio_call = aio_call
        self.iterator = iterator

    def reset(self):
        self.aio_call = None
        self.iterator = None

    async def seek(self, key: bytes):
        assert isinstance(key, bytes)
        await self.aio_call(self.iterator.seek, key)

    async def seek_for_prev(self, key: bytes):
        assert isinstance(key, bytes)
        await self.aio_call(self.iterator.seek_for_prev, key)

    async def seek_to_first(self):
        await self.aio_call(self.iterator.seek_to_first)

    async def seek_to_last(self):
        await self.aio_call(self.iterator.seek_to_last)

    async def valid(self) -> bool:
        result = await self.aio_call(self.iterator.valid)
        return result

    async def prev(self):
        await self.aio_call(self.iterator.prev)

    async def next(self):
        await self.aio_call(self.iterator.next)

    async def key(self) -> bytes:
        result = await self.aio_call(self.iterator.key)
        return result

    async def value(self) -> bytes:
        result = await self.aio_call(self.iterator.value)
        return result

    async def status(self) -> StatusT:
        status = await self.aio_call(self.iterator.status)
        return status

    async def close(self):
        await self.aio_call(self.iterator.close)


class SstFileReader(AsyncCallMixin):
    def __init__(self, options: Options = None):
        options = options or Options()
        assert isinstance(options, Options)
        self.options = options
        self.reader = RSstFileReader(options)
        super(SstFileReader, self).__init__()

    async def open(self, file_path: str) -> StatusT:
        status = await self.aio_call(self.reader.open, file_path)
        return status

    async def verify_checksum(self, read_options: ReadOptionsT = None) \
            -> StatusT:
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        status = await self.aio_call(self.reader.verify_checksum, read_options)
        return status

    async def create_iterator(self, read_options: ReadOptionsT = None) -> RocksDbIterator:
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        iterator = RIterator()
        await self.aio_call(self.reader.create_iterator, iterator, read_options)
        result = RocksDbIterator(self.aio_call, iterator)
        return result

    async def close(self):
        self.close_executor()


class SstFileWriter(AsyncCallMixin):
    def __init__(self, env_options: EnvOptions = None, options: Options = None):
        env_options = env_options or EnvOptions()
        assert isinstance(env_options, EnvOptions)
        options = options or Options()
        assert isinstance(options, Options)
        self.env_options = env_options
        self.options = options
        self.writer = RSstFileWriter(env_options, options)
        super(SstFileWriter, self).__init__()

    async def open(self, file_path: str) -> StatusT:
        status = await self.aio_call(self.writer.open, file_path)
        return status

    async def put(self, key: bytes, value: bytes) -> StatusT:
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)
        status = await self.aio_call(self.writer.put, key, value)
        return status

    async def delete(self, key: bytes) -> StatusT:
        assert isinstance(key, bytes)
        status = await self.aio_call(self.writer.delete, key)
        return status

    async def delete_range(self, key_from: bytes, key_to: bytes) -> StatusT:
        assert isinstance(key_from, bytes)
        assert isinstance(key_to, bytes)
        status = await self.aio_call(self.writer.delete_range, key_from, key_to)
        return status

    async def finish(self) -> StatusT:
        status = await self.aio_call(self.writer.finish)
        self.close_executor()
        return status


class RocksDbBackupReadonly(AsyncCallMixin):
    def __init__(self, backup=None):
        backup = backup or RBackupReadonly
        self.backup = backup()
        super(RocksDbBackupReadonly, self).__init__()

    async def open(self, options: BackupableDBOptionsNative) -> StatusT:
        assert isinstance(options, BackupableDBOptionsNative)
        status = await self.aio_call(self.backup.open, options)
        return status

    async def close(self):
        await self.aio_call(self.backup.close)
        self.backup = None
        self.close_executor()

    async def get_backup_info(self) -> List[BackupInfo]:
        result = await self.aio_call(self.backup.get_backup_info)
        return result

    async def verify_backup(self, backup_id: int) -> bool:
        assert isinstance(backup_id, int) and 0 < backup_id < 2**32
        result = await self.aio_call(self.backup.verify_backup, backup_id)
        return result
    
    async def restore_db_from_backup(self, backup_id: int, db_dir: str, wal_dir: str, restore_options: RestoreOptions = None) -> StatusT:
        assert isinstance(backup_id, int) and 0 < backup_id < 2**32
        restore_options = restore_options or RestoreOptions()
        result = await self.aio_call(self.backup.restore_db_from_backup, restore_options, backup_id, db_dir, wal_dir)
        return result


class RocksDbDbBackup(RocksDbBackupReadonly):
    def __init__(self):
        super(RocksDbDbBackup, self).__init__(backup=RBackup)

    async def purge_old_backups(self, num_backups_to_keep: int) -> StatusT:
        assert isinstance(num_backups_to_keep, int) and num_backups_to_keep > 0
        result = await self.aio_call(self.backup.purge_old_backups, num_backups_to_keep)
        return result

    async def delete_backup(self, backup_id: int) -> StatusT:
        assert isinstance(backup_id, int) and 0 < backup_id < 2 ** 32
        result = await self.aio_call(self.backup.delete_backup, backup_id)
        return result


class RocksDb(AsyncCallMixin):
    DEFAULT_COLUMN_FAMILY = 'default'

    def __init__(self):
        super(RocksDb, self).__init__()
        self._db: Optional[RDb] = None
        self._column_family_list = None
        self.enable_transaction = False
        self.enable_optimistic_transaction = False

    @property
    def column_family_list(self) -> List[RColumnFamily]:
        return list(self._column_family_list or list())

    @property
    def column_family_dict(self) -> Dict[str, RColumnFamilyT]:
        return {cf.get_name(): cf for cf in self._column_family_list}

    @property
    def default_column_family(self):
        return self.column_family_dict[self.DEFAULT_COLUMN_FAMILY]

    @classmethod
    async def load_latest_options(cls, db_path: str, options: ConfigOptions = None) -> StatusT[List[RColumnFamily]]:
        options = options or ConfigOptions()
        assert isinstance(options, ConfigOptions)
        complex_status = await asyncio.get_event_loop().run_in_executor(None, load_latest_options, options, db_path)
        status = complex_status.status
        status.result = complex_status.column_family_list
        status.options = complex_status.options
        return status

    @classmethod
    def _build(cls, rocks, path, options: OptionsT, column_family_list=None):
        column_family_list = column_family_list or list()
        if not any(cf for cf in column_family_list if cf.get_name() == cls.DEFAULT_COLUMN_FAMILY):
            column_family_list.append(RColumnFamily(cls.DEFAULT_COLUMN_FAMILY))
        rocks._column_family_list = column_family_list
        rocks._db = RDb(path, options)

    @classmethod
    async def destroy_db(cls, db_path: str, options: OptionsT = None) -> StatusT:
        options = options or Options()
        assert isinstance(options, Options)
        status = await asyncio.get_event_loop().run_in_executor(None, destroy_db, db_path, options)
        return status

    @classmethod
    async def repair_db(cls, db_path: str, options: OptionsT = None) -> StatusT:
        options = options or Options()
        assert isinstance(options, Options)
        status = await asyncio.get_event_loop().run_in_executor(None, repair_db, db_path, options)
        return status

    @classmethod
    async def open_db(cls, path, options: OptionsT = None, column_family_list=None) -> StatusT['RocksDb']:
        options = options or Options()
        assert isinstance(options, Options)
        rocks = RocksDb()
        rocks.enable_transaction = False
        rocks.enable_optimistic_transaction = False
        cls._build(rocks, path, options, column_family_list)
        column_family_list = rocks._column_family_list
        status = await rocks.aio_call(rocks._db.open_db, column_family_list)
        status.result = rocks
        return status

    @classmethod
    async def open_ttl_db(cls, path: str, ttls: List[int], column_family_list: List[RColumnFamily], read_only: bool = False, options: OptionsT = None) -> StatusT['RocksDb']:
        options = options or Options()
        assert isinstance(options, Options)
        assert isinstance(ttls, list)
        assert isinstance(column_family_list, list)
        assert len(ttls) == len(column_family_list)
        rocks = RocksDb()
        rocks.enable_transaction = False
        rocks.enable_optimistic_transaction = False
        cls._build(rocks, path, options, column_family_list)
        column_family_list = rocks._column_family_list
        status = await rocks.aio_call(rocks._db.open_db_with_ttl, column_family_list, ttls, read_only)
        status.result = rocks
        return status

    @classmethod
    async def open_db_for_readonly(cls, path, options: OptionsT = None, column_family_list=None,
                                   error_if_log_file_exist: bool = False) -> StatusT['RocksDb']:
        options = options or Options()
        assert isinstance(options, Options)
        rocks = RocksDb()
        rocks.enable_transaction = False
        rocks.enable_optimistic_transaction = False
        cls._build(rocks, path, options, column_family_list)
        column_family_list = rocks._column_family_list
        status = await rocks.aio_call(rocks._db.open_db_for_readonly, column_family_list, error_if_log_file_exist)
        status.result = rocks
        return status

    @classmethod
    async def open_transaction_db(cls, path, options: OptionsT = None, column_family_list=None) -> StatusT['RocksDb']:
        options = options or Options()
        assert isinstance(options, Options)
        rocks = RocksDb()
        rocks.enable_transaction = True
        rocks.enable_optimistic_transaction = False
        cls._build(rocks, path, options, column_family_list)
        column_family_list = rocks._column_family_list
        status = await rocks.aio_call(rocks._db.open_transaction_db, column_family_list)
        status.result = rocks
        return status

    @classmethod
    async def open_optimistic_transaction_db(cls, path, options: OptionsT = None, column_family_list=None) -> StatusT['RocksDb']:
        options = options or Options()
        assert isinstance(options, Options)
        rocks = RocksDb()
        rocks.enable_transaction = False
        rocks.enable_optimistic_transaction = True
        cls._build(rocks, path, options, column_family_list)
        column_family_list = rocks._column_family_list
        status = await rocks.aio_call(rocks._db.open_optimistic_transaction_db, column_family_list)
        status.result = rocks
        return status

    async def close(self):
        if self._column_family_list:
            for cf in self._column_family_list:
                await self.aio_call(self._db.destroy_column_family, cf)
            self._column_family_list = None
        await self.aio_call(self._db.close)
        self._db = None
        self.close_executor()

    async def create_column_family(self, column_family: RColumnFamilyT, options: ColumnFamilyOptions = None, ttl: int = 0) -> StatusT:
        if column_family.is_loaded():
            raise ValueError('column_family loaded column family handle')
        options = options or ColumnFamilyOptions()
        assert isinstance(options, ColumnFamilyOptions)
        status = await self.aio_call(self._db.create_column_family, options, column_family, ttl)
        if status.ok():
            self._column_family_list.append(column_family)
        return status

    async def drop_column_family(self, column_family: RColumnFamilyT) -> StatusT:
        if not column_family.is_loaded():
            raise ValueError('column_family without column family handle')
        status = await self.aio_call(self._db.drop_column_family, column_family)
        if status.ok():
            self._column_family_list = [cf for cf in self._column_family_list if cf != column_family]
        return status

    async def create_snapshot(self) -> SnapshotT:
        snapshot = RSnapshot()
        await self.aio_call(self._db.create_snapshot, snapshot)
        return snapshot

    async def release_snapshot(self, snapshot: SnapshotT):
        await self.aio_call(self._db.release_snapshot, snapshot)

    async def write(self, batch: RBatch, write_options: WriteOptions = None) -> StatusT:
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        assert isinstance(batch, RBatch)
        status = await self.aio_call(self._db.write, write_options, batch)
        return status

    async def put(self, key, value, write_options: WriteOptions = None, column_family: RColumnFamilyT = None) -> StatusT:
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self._db.put, write_options, column_family, key, value)
        return status

    async def get(self, key: bytes, read_options: ReadOptionsT = None, column_family: RColumnFamilyT = None) \
            -> StatusT[bytes]:
        assert isinstance(key, bytes)
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        complex_status: ComplexStatusT = await self.aio_call(self._db.get, read_options, column_family, key)
        status = complex_status.status
        status.result = complex_status.value
        return status

    async def multi_get(self, pairs: List[Tuple[bytes, RColumnFamilyT]], read_options: ReadOptionsT = None) \
            -> List[StatusT[bytes]]:
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        keys, cf_list = list(zip(*pairs))
        keys = list(keys)
        cf_list = list(cf_list)
        complex_status: ComplexStatusT = await self.aio_call(self._db.multi_get, read_options, cf_list, keys)
        status_list = complex_status.status_list
        for idx in range(len(status_list)):
            status_list[idx].result = complex_status.value_list[idx]
        return status_list

    async def delete(self, key: bytes, write_options: WriteOptions = None, column_family: RColumnFamilyT = None) -> StatusT:
        assert isinstance(key, bytes)
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self._db.delete_key, write_options, column_family, key)
        return status

    async def delete_range(self, from_key: bytes, to_key: bytes, write_options: WriteOptions = None,
                           column_family: RColumnFamily = None) -> StatusT:
        assert isinstance(from_key, bytes)
        assert isinstance(to_key, bytes)
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self._db.delete_range, write_options, column_family, from_key, to_key)
        return status

    async def create_iterator(self, read_options: ReadOptionsT = None, column_family: RColumnFamilyT = None) -> RocksDbIterator:
        read_options = read_options or ReadOptions()
        assert isinstance(read_options, ReadOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        iterator = RIterator()
        await self.aio_call(self._db.create_iterator, iterator, read_options, column_family)
        result = RocksDbIterator(self.aio_call, iterator)
        return result

    async def ingest_external_file(self, files: List[str], options: IngestExternalFileOptions = None,
                                   column_family: RColumnFamilyT = None) -> StatusT:
        options = options or IngestExternalFileOptions()
        assert isinstance(options, IngestExternalFileOptions)
        column_family = column_family or self.default_column_family
        assert isinstance(column_family, RColumnFamily)
        status = await self.aio_call(self._db.ingest_external_file, files, column_family, options)
        return status

    async def flush(self, column_family_list: List[RColumnFamily], flush_options: FlushOptions = None) -> StatusT:
        flush_options = flush_options or FlushOptions()
        assert isinstance(flush_options, FlushOptions)
        assert column_family_list
        status = await self.aio_call(self._db.flush, flush_options, column_family_list)
        return status

    async def create_backup(self, backup: RocksDbDbBackup) -> StatusT:
        assert isinstance(backup, RocksDbDbBackup) and backup.backup
        status = await self.aio_call(self._db.create_backup, backup.backup)
        return status

    async def begin_transaction(self, write_options: WriteOptions = None, txn_options: TransactionOptions = None) -> RocksDbTransaction:
        assert self.enable_transaction
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        txn_options = txn_options or TransactionOptions()
        assert isinstance(txn_options, TransactionOptions)

        transaction = RocksDbTransaction(self.aio_call, RTransaction(), self)
        await self.aio_call(self._db.begin_transaction, write_options, txn_options, transaction.transaction)
        return transaction

    async def begin_optimistic_transaction(self, write_options: WriteOptions = None, txn_options: OptimisticTransactionOptions = None) -> RocksDbTransaction:
        assert self.enable_optimistic_transaction
        write_options = write_options or WriteOptions()
        assert isinstance(write_options, WriteOptions)
        txn_options = txn_options or OptimisticTransactionOptions()
        assert isinstance(txn_options, OptimisticTransactionOptions)

        transaction = RocksDbTransaction(self.aio_call, RTransaction(), self)
        await self.aio_call(self._db.begin_optimistic_transaction, write_options, txn_options, transaction.transaction)
        return transaction

    async def release_transaction(self, transaction: RocksDbTransaction):
        assert isinstance(transaction.transaction, RTransaction)
        await self.aio_call(self._db.release_transaction, transaction.transaction)
        transaction.transaction = None
        transaction.db = None

    async def set_ttl(self, ttl: int):
        assert isinstance(ttl, int)
        await self.aio_call(self._db.set_ttl, ttl)

    async def set_column_family_ttl(self, column_family: RColumnFamily, ttl: int):
        assert isinstance(ttl, int)
        assert isinstance(column_family, RColumnFamily)
        await self.aio_call(self._db.set_ttl, column_family, ttl)


__all__ = ['RocksDb', 'DbPath', 'Options', 'ReadOptions', 'ComplexStatus', 'WriteOptions', 'RColumnFamily',
           'EnvOptions', 'RSnapshot', 'RocksDbIterator', 'RBatch', 'FlushOptions', 'SstFileWriter',
           'IngestExternalFileOptions', 'SstFileReader', 'RocksDbBackupReadonly', 'RocksDbDbBackup',
           'BackupableDBOptions', 'RocksDbTransaction', 'ColumnFamilyOptions', ]
