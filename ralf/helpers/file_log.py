from enum import Enum
from typing import List

FILENAME = "/tmp/ralf_stats/latest.txt"
DELIMITER = "|"

class QueryType(Enum):
    POINT_QUERY = "P"
    BULK_QUERY = "B"
    ANY_QUERY = "PB"

def log_query_info(query_type, table, time, latency, key=None):
    with open(FILENAME, "a+") as f:
        f.write(
            f"{query_type}{DELIMITER}"
            f"Table: {str(table)}{DELIMITER}"
            f"Key: {str(key)}{DELIMITER}"
            f"Time: {time}{DELIMITER}"           # In milliseconds
            f"Latency: {latency}\n"              # In milliseconds
        )

def filter_line(tokens: List[str],
                query_type: QueryType,
                table: str, key: str,
                min_time_stamp: float,
                max_time_stamp: float,
                min_latency: float,
                max_latency: float):
    '''
    Checks if the logged line that has been parsed into tokens fits the
    criteria specified by the other parameters.
    '''

    logged_query_type = tokens[0]
    logged_table = tokens[1]
    logged_key = tokens[2]
    logged_time = float(tokens[3])
    logged_latency = float(tokens[4])
    return logged_query_type in query_type.value and \
                (table is None or logged_table == table) and \
                (key is None or logged_key == key) and \
                (min_time_stamp < logged_time < max_time_stamp) and \
                (min_latency < logged_latency < max_latency)


def query_count_and_latency(query_type: QueryType=QueryType.ANY_QUERY,
                            table: str=None,
                            key: str=None,
                            min_time_stamp: float=-float("inf"),
                            max_time_stamp: float=float("inf"),
                            min_latency: float=-float("inf"),
                            max_latency: float=float("inf")):
    '''
    Finds total number of queries and their cumulative latency. Only counts
    queries that fit within the other specified parameters. Returns tuple of
    total count and total latency.
    '''

    total_latency, total_queries = 0, 0
    with open(FILENAME, "r") as f:
        for line in f:
            toks = line.strip().split(DELIMITER)
            if filter_line(toks, query_type, table, key, min_time_stamp,
                    max_time_stamp, min_latency, max_latency):
                logged_latency = float(toks[4])
                total_latency += logged_latency
                total_queries += 1
    return (total_latency, total_queries)

def calculate_average_latency(query_type: QueryType=QueryType.ANY_QUERY,
                              table: str=None,
                              key: str=None,
                              min_time_stamp: float=-float("inf"),
                              max_time_stamp: float=float("inf"),
                              min_latency: float=-float("inf"),
                              max_latency: float=float("inf")):
    '''
    Calculates average latency of queries with type query_type.
    Returns time in milliseconds.
    '''

    total_latency, total_queries = query_count_and_latency(query_type,
                        table=table,
                        key=key,
                        min_time_stamp=min_time_stamp,
                        max_time_stamp=max_time_stamp,
                        min_latency=min_latency,
                        max_latency=max_latency)
    return total_latency / total_queries

def calculate_total_query_latency(query_type: QueryType=QueryType.ANY_QUERY,
                                  table: str=None,
                                  key: str=None,
                                  min_time_stamp: float=-float("inf"),
                                  max_time_stamp: float=float("inf"),
                                  min_latency: float=-float("inf"),
                                  max_latency: float=float("inf")):
    '''
    Returns total latency of queries with type query_type.
    '''

    return query_count_and_latency(query_type,
                            table=table,
                            key=key,
                            min_time_stamp=min_time_stamp,
                            max_time_stamp=max_time_stamp,
                            min_latency=min_latency,
                            max_latency=max_latency)[0]

def calculate_query_count(query_type: QueryType=QueryType.ANY_QUERY,
                          table: str=None,
                          key: str=None,
                          min_time_stamp: float=-float("inf"),
                          max_time_stamp: float=float("inf"),
                          min_latency: float=-float("inf"),
                          max_latency: float=float("inf")):
    '''
    Returns number of queries with type query_type.
    '''

    return query_count_and_latency(query_type,
                            table=table,
                            key=key,
                            min_time_stamp=min_time_stamp,
                            max_time_stamp=max_time_stamp,
                            min_latency=min_latency,
                            max_latency=max_latency)[1]