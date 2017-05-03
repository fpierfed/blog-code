#!/usr/bin/env python3
"""
Task description format

task: (name, args, uuid)

uuid is only use to connect results to tasks. If the task does not need to
fetch its results, then uuid can be safely set to None. THe inplication is that
the task becomes a one-way thing with no garantees of success.
"""
import json
import os
from statistics import stdev
import threading
import timeit
import uuid
import redis


RESULTS = {}


def generate_work(n, taskq, host, port):
    r = redis.StrictRedis(host, port)
    for i in range(n):
        task_id = str(uuid.uuid4())
        RESULTS[task_id] = None

        task_desc = json.dumps(('add', (i, i), task_id))
        r.lpush(taskq, task_desc)


def fetch_results(n, resq, host, port):
    r = redis.StrictRedis(host, port)
    received = 0
    while received < n:
        _, raw = r.blpop(resq)
        (task_id, value) = json.loads(raw)
        RESULTS[task_id] = value
        received += 1


def runner(n, host, port):
    taskq = 'tasks'
    resq = 'results'

    threads = [threading.Thread(target=generate_work,
                                args=(n, taskq, host, port),
                                daemon=True),
               threading.Thread(target=fetch_results,
                                args=(n, resq, host, port),
                                daemon=True)]
    [t.start() for t in threads]
    [t.join() for t in threads]
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
