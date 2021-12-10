from operator import ne
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
OPERATOR_LOG_FILE_NAME = "operator_log.txt"
QUERY_LOG_FILE_NAME = "query_log.txt"

next_record_id_queue = deque([])
keys = [f"v_{i}" for i in range(100)]
next_key_index = 0

mm = MemoryManager()
mm.toggle_cost_aware_optimization_enabled_parameter(True)

class Source(SourceOperator):
    def __init__(self, schema):
        super().__init__(schema)

    def next(self):
        global next_key_index, key_suffix
        if next_key_index < 2 * len(keys):
            if next_key_index == len(keys):
                time.sleep(20)
            time.sleep(0.05)
            next_key_index += 1
            return [Record(user=keys[next_key_index % len(keys)], timestamp=time.time())]
        else:
            time.sleep(100)
            next_key_index = 1
            return [Record(user=keys[0], timestamp=time.time())]


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
            time.sleep(0.04)
            ec.extend(mm.set(self.name + "_" + record.user, record))
        if did_fetch_from_disk:
            with open(DISK_READ_FILE_NAME, "r") as f:
                lines = f.readlines()
                lines.split()
        for candidate_key in ec:
            if candidate_key.find("short_latency") != 0:
                with open(DISK_WRITE_FILE_NAME, "a+") as f:
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
            #time.sleep(0.00000001)
            ec.extend(mm.set(self.name + "_" + record.user, record))
        for candidate_key in ec:
            if candidate_key.find("short_latency") != 0:
                with open(DISK_WRITE_FILE_NAME, "a+") as f:
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
        with open(OPERATOR_LOG_FILE_NAME, "a+") as f:
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


ralf_server.run()

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

# ************************************************************************** #
# ************************************************************************** #
# ************************************************************************** #

time.sleep(30)

# Query the keys (equivalent to insertion in sim)
print(f"\nStarting to query keys! Key index: {next_key_index}\n")

from scipy.stats import norm
import numpy as np

gaussian_pdf = norm.pdf(np.linspace(norm.ppf(0.01), norm.ppf(0.99), len(keys)))
normalized_gaussian_pdf = gaussian_pdf / sum(gaussian_pdf)

num_queries = 1000
for query in range(num_queries):
    random_key = np.random.choice(keys, p=normalized_gaussian_pdf)

    #timed_table_point_query(Writer, random_key)
    pq_latency = asyncio.run(timed_table_point_query(writer, random_key))
    with open(QUERY_LOG_FILE_NAME, "a+") as f:
        f.write(f"PQ|{random_key}|{pq_latency}\n")

print("\nFinished querying keys!\n")
# Insert keys (equivalent to queries in sim)

time.sleep(30)

print("\nStarting to insert keys!\n")

num_insertions = 100
for insertion in range(num_insertions):
    random_key = np.random.choice(keys) + "_new"
    next_record_id_queue.append(random_key)

print("\nFinished inserting keys!\n")
time.sleep(20)
# Mix of queries and insertions
