import asyncio


class KeyLock:
    def __init__(self):
        self.d = dict()

    def acquire(self, key):
        return KeyContext(self.d, hash(key) % 64)


class KeyContext:
    def __init__(self, lock, key):
        self.key_lock = lock
        self.key = key
        if key in lock:
            lock[key]['times'] += 1
        else:
            lock[key] = dict(times=1, lock=asyncio.Lock())

    async def __aenter__(self):
        await self.key_lock[self.key]['lock'].acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.key_lock[self.key]['lock'].release()
        self.key_lock[self.key]['times'] -= 1
        if not self.key_lock[self.key]['times']:
            del self.key_lock[self.key]
        self.key_lock = None
        self.key = None


__all__ = ['KeyLock', ]
