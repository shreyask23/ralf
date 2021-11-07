import asyncio
from ralf.core import Ralf
import ray

async def _timed_table_query_helper(table, key=None):
    operators = table.pool.handles
    if key == None:
        queries = ray.get([operator.timed_get_all.remote() for operator in operators])
    else:
        queries = ray.get([operator.timed_get.remote(key) for operator in operators])
    earliest_start_time = sorted(queries, key=lambda timed_result: timed_result[0])[0][0]
    latest_end_time = sorted(queries, key=lambda timed_result: timed_result[1])[-1][1]
    return latest_end_time - earliest_start_time

async def timed_table_point_query(table, key):
    return _timed_table_query_helper(table, key=key)

async def timed_table_bulk_query(table):
    return _timed_table_query_helper(table)