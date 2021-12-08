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

class Source(SourceOperator):
    def __init__(self, schema):
        super().__init__(schema)

    def next(self):
        time.sleep(0.01)
        user_id = random.randint(1, 10)
        return [Record(user=str(user_id), timestamp=time.time())]

from ralf import Schema

source_schema = Schema(
    primary_key="user", columns={"user": str, "timestamp": time}
)

source = ralf_server.create_source(Source, args=(source_schema,))

from collections import defaultdict
import numpy as np

from ralf import Operator, Record

class LongLatency(Operator):
    def __init__(self, schema):
        super().__init__(schema)

    def on_record(self, record: Record):
        time.sleep(3)
        # get() record w/ memory manager
        # if there are eviction candidates, simulate writing to disk
        # if we fetched from disk, simulate writing to disk
        output_record = record
        return output_record

long_latency_schema = Schema(
    primary_key="user", columns={"user": str, "timestamp": time}
)
long_latency = source.map(LongLatency, args=(long_latency_schema,))

class ShortLatency(Operator):
    def __init__(self, schema):
        super().__init__(schema)

    def on_record(self, record: Record):
        time.sleep(0.00000001)
        # get() record w/ memory manager
        output_record = record
        return output_record

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
            f.write(f"st:{record.start_timestamp} | et: {record.end_timestamp} | latency: {record.end_timestamp - record.start_timestamp}")
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


# In[ ]:


ralf_client.point_query(table_name="source", key="1")


# In[ ]:


ralf_client.point_query(table_name="average", key="1")


# In[ ]:


ralf_client.bulk_query(table_name="average")


# # Advanced: Maintaining user vectors 
# Now that we've setup a simple feature table and run some queries, we can create a more realistic feature table: a user vector representing their movie tastes. 
# 
# In this example, we'll assume we already have pre-computed movie vectors which are held constant. User vectors are updated over time as new rating information is recieved using a simple ALS model. 

# In[ ]:


import sys
# get_ipython().system('{sys.executable} -m pip install pandas')


# In[ ]:


# get_ipython().system('wget https://raw.githubusercontent.com/feature-store/risecamp-2021/main/user_active_time.csv')
# get_ipython().system('wget https://raw.githubusercontent.com/feature-store/risecamp-2021/sarah-als/als.py')
# get_ipython().system('wget https://raw.githubusercontent.com/feature-store/risecamp-2021/sarah-als/movie_matrix.csv')


# In[ ]:


from als import ALSModel

class UserVector(Operator):
    
    def __init__(self, schema): 
        super().__init__(schema)
        self.model = ALSModel(.1)
    
    def on_record(self, record: Record):
        updated_user_vector = self.model.als_step(record.user, record.movie, record.rating)
        output_record = Record(user=record.user, user_vector=updated_user_vector)
        return output_record  
    
user_schema = Schema(
    primary_key="user", columns={"user": str, "user_vector": np.array}
)
user_vectors = source.map(UserVector, args=(user_schema,))
user_vectors.as_queryable("user_vectors")


# In[ ]:


ralf_server.run()


# Now, we can query our table to view the user vectors. Re-run this cell to observe how the user vectors change over time. 

# In[ ]:


ralf_client.bulk_query(table_name="user_vectors")


# ## Prioritizing Active Users 
# Ralf allows for key-level prioritization policies. Say that we want to prioritize computing updates to user vectors for users who especially active. We can use activity data to implement a prioritized lottery scheduling policy. 

# In[ ]:


import pandas as pd

user_activity = pd.read_csv("user_active_time.csv")
user_activity


# For example, we can set the subsampling rate of the data to be inversely proportional to how active the user is. 

# In[ ]:


class SampleActiveUsers(LoadSheddingPolicy):
    
    def __init__(self, user_activity_csv):
        user_activity = pd.read_csv("user_active_time.csv")
        self.weights = user_activity.set_index("user_id")["activity"].to_dict()

    def process(record: Record): 
        return random.random() < self.weights[record.user]


# Alternatively, we can create a key prioritization policy which prioritizes keys uses lottery scheduling. 

# In[ ]:


from ralf import PrioritizationPolicy
from typing import List

class LotteryScheduling(PrioritizationPolicy): 
    
    def __init__(self, user_activity_csv): 
        user_activity = pd.read_csv(user_activity_csv)
        self.weights = user_activity.set_index("user_id")["activity"].to_dict()
        
    def choose(self, keys: List): 
        # TODO: implement prioritized lottery scheduling 
        return random.choose(keys)

user_vectors.add_prioritization_policy(LotteryScheduling, "user_active_time.csv")


# In[ ]:


source.debug_state()


# In[ ]:

# print(user_vectors.get_all())
# print(ralf_server.pipeline_view())

# print(ralf_server.r)
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

print(asyncio.run(timed_table_bulk_query(user_vectors)))