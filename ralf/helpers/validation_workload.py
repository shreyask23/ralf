from memory_manager import MemoryManager
from collections import deque

import sys
from ralf import Ralf

# Launch RALF
ralf_server = Ralf()

from ralf.operators.source import SourceOperator
from ralf import Record
import random
import time

DISK_WRITE_FILE_NAME = "disk_write.txt"
DISK_READ_FILE_NAME = "disk_read.txt"
LOG_FILE_NAME = "log.txt"

next_record_id_queue = deque([])

mm = MemoryManager()
mm.toggle_cost_aware_optimization_enabled_parameter(True)

class Source(SourceOperator):
    def __init__(self, schema):
        super().__init__(schema)

    def next(self):
        while len(next_record_id_queue) == 0:
            time.sleep(5)
        user_id = next_record_id_queue.popleft()
        return [Record(user=str(user_id), timestamp=time.time())]


from ralf import Schema

source_schema = Schema(
    primary_key="user", columns={"user": str, "timestamp": time}
)

source = ralf_server.create_source(Source, args=(source_schema,))

from collections import defaultdict

from ralf import Operator, Record

class LongLatency(Operator):
    def __init__(self, schema):
        super().__init__(schema)
        self.name = "long_latency"

    def on_record(self, record: Record):
        mem_record, did_fetch_from_disk, ec = mm.get(self.name + "_" + record.user)
        if mem_record is None:
            time.sleep(3)
            ec.extend(mm.set(self.name + "_" + record.user, record))
        if did_fetch_from_disk:
            with open(DISK_READ_FILE_NAME, "r") as f:
                lines = f.readlines()
                lines.split()
        for candidate_key in range(len(ec)):
            if candidate_key.find("short_latency") != 0:
                with open(DISK_WRITE_FILE_NAME, "w+") as f:
                    f.write("Here's a new record!\n")
        return record

long_latency_schema = Schema(
    primary_key="user", columns={"user": str, "timestamp": time}
)
long_latency = source.map(LongLatency, args=(long_latency_schema,))

class ShortLatency(Operator):
    def __init__(self, schema):
        super().__init__(schema)
        self.name = "short_latency"

    def on_record(self, record: Record):
        mem_record, _, ec = mm.get(self.name + "_" + record.user)
        if mem_record is None:
            time.sleep(0.00000001)
            ec.extend(mm.set(self.name + "_" + record.user, record))
        for candidate_key in range(len(ec)):
            if candidate_key.find("short_latency") != 0:
                with open(DISK_WRITE_FILE_NAME, "w+") as f:
                    f.write("Here's a new record!\n")
        return record

short_latency_schema = Schema(
    primary_key="user", columns={"user": str, "timestamp": time}
)
short_latency = long_latency.map(ShortLatency, args=(short_latency_schema,))

class SinkOperator(Operator):
    def __init__(self, schema):
        super().__init__(schema)

    def on_record(self, record: Record):
        output_record = Record(user=record.user, start_timestamp=record.timestamp, end_timestamp=time.time())
        return output_record

sink_latency_schema = Schema(
    primary_key="user", columns={"user": str, "start_timestamp": time, "end_timestamp": time}
)
sink_operator = short_latency.map(SinkOperator, args=(sink_latency_schema,))

class Writer(Operator):
    def __init__(self, schema):
        super().__init__(schema)

    def on_record(self, record: Record):
        with open(LOG_FILE_NAME, "a+") as f:
            f.write(f"st:{record.start_timestamp} | et: {record.end_timestamp} | latency: {record.end_timestamp - record.start_timestamp}\n")
        return record

writer_latency_schema = Schema(
    primary_key="user", columns={"user": str, "start_timestamp": time, "end_timestamp": time}
)
writer = sink_operator.map(Writer, args=(writer_latency_schema,))


writer.as_queryable("writer")

# ************************************************************************** #
# ************************************************************************** #
# ************************************************************************** #


# We can apply our updates to the pipeline by running: 

# In[ ]:


ralf_server.run()


# ## Creating a `ralf` Client 
# Now that we have a simple pipeline, we can query the ralf server for features. 

# In[ ]:


from ralf import RalfClient
ralf_client = RalfClient()


import asyncio
import ray

async def _timed_table_query_helper(table, key=None):
    operators = table.pool.handles
    if key == None:
        queries = ray.get([operator.timed_get_all.remote() for operator in operators])
    else:
        queries = ray.get([operator.timed_get.remote(key) for operator in operators])
    # await asyncio.wait(queries)
    print(f"Queries type: {type(queries)} -- queries: {queries}")
    earliest_start_time = sorted(queries, key=lambda timed_result: timed_result[0])[0][0]
    latest_end_time = sorted(queries, key=lambda timed_result: timed_result[1])[-1][1]

    print(f"Time: {latest_end_time - earliest_start_time}")
    return latest_end_time - earliest_start_time

async def timed_table_point_query(table, key):
    return await _timed_table_query_helper(table, key=key)

async def timed_table_bulk_query(table):
    return await _timed_table_query_helper(table)

# print(asyncio.run(timed_table_bulk_query(user_vectors)))

# delay = 5
# print(f"Waiting {delay} seconds before bulk querying from writer table")
# time.sleep(delay)
# print(ralf_client.bulk_query(table_name="writer"))

# print(ralf_client.point_query(table_name="writer", key="1"))

# ************************************************************************** #
# ************************************************************************** #
# ************************************************************************** #

keys = ["v_{i}" for i in range(1000000000)]

for key in keys:
    next_record_id_queue.append(key)

with open(LOG_FILE_NAME, "a+") as f:
    f.write("\nFinished loading the table!\n")

# Query the keys (equivalent to insertion in sim)
with open(LOG_FILE_NAME, "a+") as f:
    f.write("\nStarting to query keys!\n")

from scipy.stats import norm
import numpy as np

gaussian_pdf = norm.pdf(np.linspace(norm.ppf(0.01), norm.ppf(0.99), len(keys)))
normalized_gaussian_pdf = gaussian_pdf / sum(gaussian_pdf)

num_queries = 1000
for query in range(num_queries):
    random_key = np.random.choice(keys, p=normalized_gaussian_pdf)

    timed_table_point_query(Writer, random_key)
    pq_latency = ralf_client.point_query(table_name="writer", key="1")
    with open(LOG_FILE_NAME, "a+") as f:
        f.write(f"PQ | Key: {random_key} | Latency: {pq_latency}\n")

with open(LOG_FILE_NAME, "a+") as f:
    f.write("\nFinished querying keys!\n")

# Insert keys (equivalent to queries in sim)

with open(LOG_FILE_NAME, "a+") as f:
    f.write("\nStarting to insert keys!\n")

num_insertions = 1000
for insertion in range(num_insertions):
    random_key = np.random.choice(keys) + "_new"
    next_record_id_queue.append(random_key)

with open(LOG_FILE_NAME, "a+") as f:
    f.write("\nFinished inserting keys!\n")

# Mix of queries and insertions
