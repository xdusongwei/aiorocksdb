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
