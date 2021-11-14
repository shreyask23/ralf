import asyncio
from ralf.core import Ralf
import ray
from file_log import QueryType, log_query_info
import time

def time_milliseconds(t: int=None):
    if t is None:
        return time.time() * 1000  # Return current time
    else:
        return t * 1000

async def _timed_table_query_helper(table, key=None):
    '''
    Returns query latency in seconds.
    '''

    operators = table.pool.handles
    if key == None:
        queries = ray.get([operator.timed_get_all.remote() for operator in operators])
    else:
        queries = ray.get([operator.timed_get.remote(key) for operator in operators])
    earliest_start_time = sorted(queries, key=lambda timed_result: timed_result[0])[0][0]
    latest_end_time = sorted(queries, key=lambda timed_result: timed_result[1])[-1][1]
    return latest_end_time - earliest_start_time

async def timed_table_point_query(table, key, time=None):
    '''
    Returns query latency in milliseconds.
    '''

    if time is None:
        time = time_milliseconds()
    latency = time_milliseconds(_timed_table_query_helper(table, key=key))
    log_query_info(QueryType.POINT_QUERY, table, time, latency, key=key)
    return latency

async def timed_table_bulk_query(table, time=None):
    '''
    Returns query latency in milliseconds.
    '''

    if time is None:
        time = time_milliseconds()
    latency = time_milliseconds(_timed_table_query_helper(table))
    log_query_info(QueryType.BULK_QUERY, table, time, latency)
    return latency