#!/usr/bin/env python3
import base64
import json
import timeit
import msgpack


data = [list(range(128)), 'foo']


print(timeit.timeit('msgpack.packb(data)', globals=globals()))
print(timeit.timeit('msgpack.dumps(data)', globals=globals()))
print(timeit.timeit('base64.b64encode(msgpack.packb(data))',
                    globals=globals()))
print(timeit.timeit('json.dumps(data)', globals=globals()))
