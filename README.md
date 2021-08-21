aiorocksdb
==========


操作 rocksdb 的 python 层，需要在环境中事先编译安装数据库

## 基本功能

基本功能大多是按照 rocksdb api 接口的设计参照出来的接口函数 

### 打开/关闭/删除数据库

普通方式的打开数据库
```python
from aiorocksdb.rocks_db import *


async def main():
    option = Options()
    option.create_if_missing = True
    s = await RocksDb.open_db('dbPathName', option)
    assert s.ok()
    db = s.result
```

只读打开数据库
```python
s = await RocksDb.open_db_for_readonly('dbPathName', option)
```

事物模式打开数据库
```python
s = await RocksDb.open_transaction_db('dbPathName', option)
```

乐观事物模式打开数据库
```python
s = await RocksDb.open_optimistic_transaction_db('dbPathName', option)
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
    option = Options()
    option.create_if_missing = True
    option.create_missing_column_families = True
    s = await RocksDb.open_db('dbPathName', option, [user_cf, order_cf, ])
    assert s.ok()
    db = s.result
```

创建列族


删除列族


读指定的键

```python
user_cf = RColumnFamily('user')
s = await db.get(b'userA', column_family=user_cf)
```

写入指定的键数据

```python
user_cf = RColumnFamily('user')
s = await db.put(b'userA', b'dataA', column_family=user_cf)
```

删除键

```python
s = await db.delete(b'keyA')
assert s.ok()

s = await db.delete_range(b'keyA', b'keyZ')
assert s.ok()
```


### 批量操作键





### 使用迭代器读取键

从头到尾完全遍历

反向遍历

设置开始/结束键遍历



### 快照


### 事物操作


### sst导出/导入


### 备份

## 高级扩展
通过基本功能提供的参照接口，结合 python 特点进行了部分改进


### 友好的迭代器

### 特定键序列化


### 类 Redis 形式的方法
