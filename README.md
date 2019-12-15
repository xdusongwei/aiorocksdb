aiorocksdb
==========


```python
import asyncio
import aiorocksdb


async def main():
    async with aiorocksdb.RocksDb('test.db') as rocksdb:
        assert not (await rocksdb.get('new_key'))
        await rocksdb.put('new_key', b'new_value')
        assert (await rocksdb.get('new_key')) == b'new_value'
        await rocksdb.delete('new_key')


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

```

List commands:

```python
import aiorocksdb


async def list_commands():
    async with aiorocksdb.RocksDb('test.db') as rocksdb:
        await rocksdb.lpush('list', b'hello')
        await rocksdb.lpush('list', b'world')
        value = await rocksdb.rpop('list')
        await rocksdb.delete_key('list')

```

Set commands:

```python
import aiorocksdb


async def set_commands():
    async with aiorocksdb.RocksDb('test.db') as rocksdb:
        await rocksdb.sadd('set', 'keyA')
        await rocksdb.sadd('set', 'keyB')
        is_true = await rocksdb.sismember('set', 'keyB')
        members = await rocksdb.smembers('set')
        await rocksdb.srem('set', 'KeyB')
```


Sorted Set commands:


```python
import aiorocksdb


async def sorted_set_commands():
    async with aiorocksdb.RocksDb('test.db') as rocksdb:
        await rocksdb.zadd('sortedSet', 1, 'apple')
        await rocksdb.zadd('sortedSet', 2, 'banana')
        is_one = await rocksdb.zscore('sortedSet', 'apple')
        await rocksdb.zrem('sortedSet', 'banana')
        is_one = await rocksdb.zcard('sortedSet')
```