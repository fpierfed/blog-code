#!/usr/bin/env python3
import asyncio
import os
import aioredis
import uvloop


class Worker:
    def __init__(self, host='localhost', port=6379, inq='tasks',
                 outq='results', loop=None):
        self.host = host
        self.port = port
        self.inqueue = inq
        self.outqueue = outq
        self.done = False
        # print('Worker configured and ready to go')

    async def run(self, loop):
        # print('Worker starting internal loop')

        redis = await aioredis.create_redis((self.host, self.port), loop=loop)
        while not self.done:
            # print('Wainting for work')

            _, task_description = await redis.blpop(self.inqueue)
            # print('Got work to do')

            result = await self.process(task_description)
            # print('Work done')

            # print(task_description, result)

            await redis.lpush(self.outqueue, result)
            # print('Sending results back')

    async def process(self, task_description):
        return task_description


if __name__ == '__main__':
    n = 20000
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', 6379)
    nw = 2

    workers = [Worker(host=redis_host, port=redis_port) for _ in range(nw)]

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    ft = asyncio.gather(*[asyncio.ensure_future(w.run(loop)) for w in workers])
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        ft.cancel()
