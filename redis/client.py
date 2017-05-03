#!/usr/bin/env python3
"""
Task description format

task: (name, args, uuid)

uuid is only use to connect results to tasks. If the task does not need to
fetch its results, then uuid can be safely set to None. THe inplication is that
the task becomes a one-way thing with no garantees of success.
"""
import asyncio
import json
import os
from statistics import stdev
import timeit
import uuid
import aioredis
import uvloop


RESULTS = {}


async def generate_work(n, taskq, host, port, loop):
    redis = await aioredis.create_redis((host, port), loop=loop)
    for i in range(n):
        task_id = str(uuid.uuid4())
        RESULTS[task_id] = None

        task_desc = json.dumps(('add', (i, i), task_id))
        await redis.lpush(taskq, task_desc)


async def fetch_results(n, resq, host, port, loop):
    redis = await aioredis.create_redis((host, port), loop=loop)
    received = 0
    while received < n:
        _, raw = await redis.blpop(resq)
        (task_id, value) = json.loads(raw)
        RESULTS[task_id] = value
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
