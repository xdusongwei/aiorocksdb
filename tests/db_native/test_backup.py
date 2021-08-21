import pytest
from aiorocksdb.rocks_db import *


@pytest.mark.asyncio
async def test_backup():
    await RocksDb.destroy_db('db_test_native')
    
    backup = RocksDbDbBackup()
    s = await backup.open(BackupableDBOptions('db_test_backup_folder'))
    assert s.ok()

    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('db_test_native', option)
    assert s.ok()
    r = s.result

    s = await r.put(b'ka', b'va')
    assert s.ok()
    s = await r.create_backup(backup)
    assert s.ok()
    s = await r.put(b'kb', b'vb')
    assert s.ok()
    s = await r.create_backup(backup)
    assert s.ok()

    s = await backup.purge_old_backups(2)
    assert s.ok()
    info_list = await backup.get_backup_info()
    assert len(info_list) == 2
    s = await backup.delete_backup(info_list[0].backup_id)
    assert s.ok()
    await r.close()

    s = await backup.restore_db_from_backup(info_list[1].backup_id, 'db_test_native', 'db_test_native')
    assert s.ok()

    await backup.close()

    # test readonly backup
    backup = RocksDbBackupReadonly()
    s = await backup.open(BackupableDBOptions('db_test_backup_folder'))
    assert s.ok()
    info_list = await backup.get_backup_info()
    s = await backup.verify_backup(info_list[0].backup_id)
    assert s.ok()
    await backup.close()


