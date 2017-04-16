#!/usr/bin/env python3
import asyncio
import os
from statistics import stdev
import timeit
import redis
import aioredis
import uvloop


def test_sync(n, host, port):
    r = redis.StrictRedis(host, port, decode_responses=True)
    for i in range(n):
        key = f'test_sync:keys:{i}'
        value = f'{i}'

        r.set(key, value)
        fetched = r.get(key)
        assert value == fetched


def test_sync_pipe(n, host, port):
    r = redis.StrictRedis(host, port, decode_responses=True)
    for i in range(n):
        key = f'test_sync_pipe:keys:{i}'
        value = f'{i}'

        pipe = r.pipeline()
        pipe.set(key, value)
        pipe.get(key)
        _, fetched = pipe.execute()
        assert value == fetched


async def test_async(n, host, port, cid, loop):
    r = await aioredis.create_redis((host, port), loop=loop)
    for i in range(n):
        key = f'test_async:keys:{cid}:{i}'
        value = f'{i}'

        await r.set(key, value)
        fetched = await r.get(key)
        assert fetched.decode('utf-8') == value
    r.close()
    await r.wait_closed()


async def test_async_pipe(n, host, port, cid, loop):
    r = await aioredis.create_redis((host, port), loop=loop)
    for i in range(n):
        key = f'test_async:keys:{cid}:{i}'
        value = f'{i}'

        pipe = r.pipeline()
        pipe.set(key, value)
        pipe.get(key)
        _, fetched = await pipe.execute()
        assert fetched.decode('utf-8') == value
    r.close()
    await r.wait_closed()


def async_runner(n, host, port, nworkers=100):
    def closest_divistor(n, d):
        while n % d:
            d += 1
        return d

    nw = closest_divistor(n, nworkers)

    loop = asyncio.get_event_loop()
    tasks = [asyncio.ensure_future(test_async(n // nw, host, port, i, loop))
             for i in range(nw)]
    loop.run_until_complete(asyncio.wait(tasks))


def async_runner_uvloop(n, host, port, nworkers=100):
    def closest_divistor(n, d):
        while n % d:
            d += 1
        return d

    nw = closest_divistor(n, nworkers)

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    tasks = [asyncio.ensure_future(test_async(n // nw, host, port, i, loop))
             for i in range(nw)]
    loop.run_until_complete(asyncio.wait(tasks))


if __name__ == '__main__':
    n = 20000
    num_repeats = 5
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)

    print(f'Testing {n} get-set operations')
    print(f'{num_repeats} repeats')

    for fname in ('test_sync', 'test_sync_pipe',
                  'async_runner', 'async_runner_uvloop'):

        os.system(f'/usr/local/bin/redis-cli -h {redis_host} flushall')

        t = timeit.Timer(f'{fname}(n, redis_host, redis_port)',
                         globals=globals())
        times = t.repeat(num_repeats, 1)
        ops = [n / t for t in times]

        print(f'{fname}: best  - {max(ops):.02f} get+set/s')
        print(f'{fname}: worst - {min(ops):.02f} get+set/s')
        print(f'{fname}: stdev - {stdev(ops):.02f}')
        print('---')
