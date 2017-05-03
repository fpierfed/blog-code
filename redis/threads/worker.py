#!/usr/bin/env python3
import json
import os
import threading
import redis


class Worker:
    def __init__(self, host='localhost', port=6379, inq='tasks',
                 outq='results', loop=None):
        self.host = host
        self.port = port
        self.inqueue = inq
        self.outqueue = outq
        self.done = False
        # print('Worker configured and ready to go')

    def run(self):
        # print('Worker starting internal loop')

        r = redis.StrictRedis(self.host, self.port)
        while not self.done:
            # print('Wainting for work')

            _, task_description = r.blpop(self.inqueue)
            # print('Got work to do')

            result = self.process(task_description)
            # print('Work done')

            # print(task_description, result)

            r.lpush(self.outqueue, result)
            # print('Sending results back')

    def process(self, task_description):
        (name, args, task_id) = json.loads(task_description)
        return json.dumps((task_id, getattr(self, name)(*args)))

    def add(self, x, y):
        return x + y


if __name__ == '__main__':
    n = 20000
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)
    nw = 2

    workers = [Worker(host=redis_host, port=redis_port) for _ in range(nw)]
    threads = [threading.Thread(target=w.run) for w in workers]
    [t.start() for t in threads]
    [t.join() for t in threads]
