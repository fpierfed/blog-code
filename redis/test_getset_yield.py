#!/usr/bin/env python3
from collections import deque
import os
from statistics import stdev
import timeit
import redis


def test_sync(n, host, port, cid=0):
    r = redis.StrictRedis(host, port, decode_responses=True)
    for i in range(n):
        key = f'test_sync:keys:{cid}:{i}'
        value = f'{i}'

        r.set(key, value)
        fetched = r.get(key)
        assert value == fetched


def test_sync_yield(n, host, port, cid=0):
    r = redis.StrictRedis(host, port, decode_responses=True)
    for i in range(n):
        key = f'test_sync:keys:{cid}:{i}'
        value = f'{i}'

        r.set(key, value)
        fetched = r.get(key)
        assert value == fetched
        yield i


def async_runner(n, host, port, nworkers=10):
    def closest_divistor(n, d):
        while n % d:
            d += 1
        return d

    nw = closest_divistor(n, nworkers)

    # These are nw workers/generator funcions: no work has been done yet.
    workers = deque([test_sync_yield(n // nw, host, port, i)
                     for i in range(nw)])

    # Now start the "main loop": run each worker until they yield. When they
    # do, simply go to the next one. If they raise a StopIteration Exception,
    # pop them from the list and go to the next one. As soon as we have no more
    # workers, return.
    while workers:
        w = workers.popleft()
        try:
            next(w)
        except StopIteration:
            # Discard worker and go to the next.
            continue
        # Pu the worker to the end of the list.
        workers.append(w)
    return


if __name__ == '__main__':
    n = 20000
    num_repeats = 5
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)

    print(f'Testing {n} get-set operations')
    print(f'{num_repeats} repeats')

    for fname in ('async_runner', 'test_sync'):
        os.system(f'/usr/local/bin/redis-cli -h {redis_host} flushall')

        t = timeit.Timer(f'{fname}(n, redis_host, redis_port)',
                         globals=globals())
        times = t.repeat(num_repeats, 1)
        ops = [n / t for t in times]

        print(f'{fname}: best  - {max(ops):.02f} get+set/s')
        print(f'{fname}: worst - {min(ops):.02f} get+set/s')
        print(f'{fname}: stdev - {stdev(ops):.02f}')
        print('---')
