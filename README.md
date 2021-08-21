aiorocksdb
==========


操作 rocksdb 的 python 层，需要在环境中事先编译安装数据库

## 基本功能

基本功能大多是按照 rocksdb api 接口的设计参照出来的接口函数，参数定义 rocksdb 项目提供的头文件。

### 打开/关闭/删除数据库

默认方式的打开数据库
```python
from aiorocksdb.rocks_db import *


async def main():
    options = Options()
    options.create_if_missing = True
    status = await RocksDb.open_db('dbPathName', options=options)
    assert status.ok()
    db = status.result
```

只读打开数据库
```python
status = await RocksDb.open_db_for_readonly('dbPathName', options=options)
```

事物模式打开数据库
```python
status = await RocksDb.open_transaction_db('dbPathName', options=options)
```

乐观事物模式打开数据库
```python
status = await RocksDb.open_optimistic_transaction_db('dbPathName', options=options)
```

打开 TTL 模式的数据库
```python
status = await RocksDb.open_ttl_db('dbPathName', options=options)
```

关闭数据库
```python
await db.close()
```

删除数据库
```python
from aiorocksdb.rocks_db import *


async def main():
    await RocksDb.destroy_db('dbPathName')
```


### 列族/读写键

打开数据库时设置列族范围
```python
from aiorocksdb.rocks_db import *


async def main():
    user_cf = RColumnFamily('user')
    order_cf = RColumnFamily('order', ColumnFamilyOptions())
    column_family_list = [user_cf, order_cf, ]
    options = Options()
    options.create_if_missing = True
    options.create_missing_column_families = True
    status = await RocksDb.open_db('dbPathName', options=options, column_family_list=column_family_list)
    assert status.ok()
    db = status.result
```

创建列族
```python
status = await db.create_column_family('newCF')
```

删除列族
```python
status = await db.drop_column_family('newCF')
```

读指定的键
```python
user_cf = RColumnFamily('user')
status = await db.get(b'userA', column_family=user_cf)
```

写入指定的键数据
```python
user_cf = RColumnFamily('user')
status = await db.put(b'userA', b'dataA', column_family=user_cf)
```

删除键
```python
status = await db.delete(b'keyA')

status = await db.delete_range(b'keyA', b'keyZ')
```


### 批量操作键
```python
batch = RBatch()
batch.put(b'Kx', b'Vx', db.default_column_family)
batch.put(b'Ky', b'Vy', db.default_column_family)
batch.delete_key(b'Kz', db.default_column_family)
status = await db.write(batch)
assert status.ok()
```


### 使用迭代器读取键
```python
iterator = await db.create_iterator()

await iterator.seek(b'keyStart')
valid = await iterator.valid()
assert valid is True
key = await iterator.key()
print(key)
await iterator.next()
...
await iterator.next()
valid = await iterator.valid()
assert valid is False
status = await iterator.status()
assert status.ok()

await iterator.close()
```

### 快照
```python
snapshot = await db.create_snapshot()
options = ReadOptions()
snapshot.set_read_options(options)
status = await db.get(b'key', options)
assert status.ok()
value = status.result
await db.release_snapshot(snapshot)
```

### 事物操作
```python
transaction = await db.begin_transaction()
status = await transaction.put(b'kA', b'vA')
if status.ok():
    status = await transaction.commit()
else:
    status = await transaction.rollback()
await db.release_transaction(transaction)
```

### sst导出/导入
```python
sst_writer = SstFileWriter()
status = await sst_writer.open('empty.sst')
status = await sst_writer.put(b'kA', b'vA')
assert status.ok()
status = await sst_writer.put(b'kB', b'vB')
assert status.ok()
status = await sst_writer.delete(b'kC')
assert status.ok()
status = await sst_writer.delete_range(b'kA', b'kB')
assert status.ok()
status = await sst_writer.finish()


sst_reader = SstFileReader()
status = await sst_reader.open('filename.sst')
status = await sst_reader.verify_checksum()
it = await sst_reader.create_iterator()
...
await it.close()
await sst_reader.close()


ingest_options = IngestExternalFileOptions()
status = await db.ingest_external_file(['import.sst', ], ingest_options)
```

### 备份
```python
backup = RocksDbDbBackup()
status= await backup.open(BackupableDBOptions('backupPathName'))
status = await db.create_backup(backup)


status = await backup.purge_old_backups(2)


info_list = await backup.get_backup_info()
status = await backup.delete_backup(info_list[0].backup_id)


info_list = await backup.get_backup_info()
s = await backup.restore_db_from_backup(info_list[0].backup_id, db_dir='dbPathName', )


await backup.close()
```

## 高级扩展
通过基本功能提供的参照接口，结合 python 特点进行了部分改进
```python
from aiorocksdb.rocks_db import *
from aiorocksdb.extension import *
from aiorocksdb.batch import *


options = Options()
options.create_if_missing = True
async with Db(Db.open_db('dbPathName', options=options)) as db:
    cf = db['default']
    d = {
        b'a1': b'a1',
        b'a2': b'a2',
        b'a3': b'a3',
        b'a5': b'a5',
        b'b1': b'b1',
        b'b2': b'b2',
        b'b3': b'b3',
    }
    
    for k, v in d.items():
        await cf.put(k, v)

    async with Batch(db):
        cf = db['default']
        for k, v in d.items():
            await cf.put(k, v)
```

### 友好的迭代器
```python
from aiorocksdb.iterator import *


async with Iterator.range(cf) as it:
    async for k, v in it:
        print(k, v)


async with Iterator.range(cf, desc=True) as it:
    async for k, v in it:
        print(k, v)


async with Iterator.range(cf, start=b'a3', stop=b'b1') as it:
    async for k, v in it:
        print(k, v)


async with Iterator.prefix(cf, prefix=b'a') as it:
    async for k, v in it:
        print(k, v)
```

### 特定键序列化
```python
import json
import msgpack
from aiorocksdb.rocks_db import *
from aiorocksdb.extension import *
from aiorocksdb.codec import *


class JsonCodec(Codec):
    def loads(self, data: bytes):
        data = data.decode('utf8')
        return json.loads(data)

    def dumps(self, data):
        return json.dumps(data).encode('utf8')
    

codec_device = Codec(prefix=b'device', loads=msgpack.loads, dumps=msgpack.dumps)
codec_animal = JsonCodec(prefix=b'animal')
codec_list = [codec_device, codec_animal, ]
options = Options()
options.create_if_missing = True
options.create_missing_column_families = True
codec_cf = RColumnFamily('codec')
async with Db(Db.open_db('dbPathName', options=options, column_family_list=[codec_cf, ]), codec_list=codec_list) as db:
    cf = db[codec_cf]
    
    k = b'device:phone'
    v = {
        'network': 'WiFi',
        'memory': 4096,
    }
    await cf.put(k, v)
    
    k = b'animal:dog'
    v = {
        'color': 'black',
        'legs': 4,
    }
    await cf.put(k, v)
    
    k = b'foo'
    v = b'bar'
    await cf.put(k, v)
    
    phone = await cf.get(b'device:phone')
    assert phone == {
        'network': 'WiFi',
        'memory': 4096,
    }
    
    dog = await cf.get(b'animal:dog')
    assert dog == {
        'color': 'black',
        'legs': 4,
    }
    
    bar = await cf.get(b'foo')
    assert bar == b'bar'
```


### 类 Redis 形式的方法
