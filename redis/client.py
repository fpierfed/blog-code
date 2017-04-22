#!/usr/bin/env python3
import asyncio
import os
from statistics import stdev
import timeit
import aioredis
import uvloop


async def generate_work(n, taskq, host, port, loop):
    redis = await aioredis.create_redis((host, port), loop=loop)
    for i in range(n):
        await redis.lpush(taskq, i)


async def fetch_results(n, resq, host, port, loop):
    redis = await aioredis.create_redis((host, port), loop=loop)
    received = 0
    while received < n:
        _, value = await redis.blpop(resq)
        assert 0 <= int(value) < n
        received += 1


def runner(n, host, port):
    taskq = 'tasks'
    resq = 'results'

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.wait((generate_work(n, taskq, host, port, loop),
                      fetch_results(n, resq, host, port, loop))))
    return


if __name__ == '__main__':
    n = 20000
    num_repeats = 5
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)

    print(f'Testing {n} * 2 lpush/lpop operations')
    print(f'{num_repeats} repeats')

    for fname in ('runner', ):
        os.system(f'/usr/local/bin/redis-cli -h {redis_host} flushall')

        t = timeit.Timer(f'{fname}(n, redis_host, redis_port)',
                         globals=globals())
        times = t.repeat(num_repeats, 1)
        ops = [n / t for t in times]

        print(f'{fname}: best  - {max(ops):.02f} tasks/s')
        print(f'{fname}: worst - {min(ops):.02f} tasks/s')
        print(f'{fname}: stdev - {stdev(ops):.02f}')
        print('---')
