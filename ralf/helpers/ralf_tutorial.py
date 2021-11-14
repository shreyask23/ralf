#!/usr/bin/env python
# coding: utf-8

# # Introduction 
# This tutorial will go through how to setup a featurization pipeline in `ralf`. We'll setup a pipeline for computing user features given a data stream of user ratings. The features are defined as `Table` objects which are incrementally maintained by `ralf` as new data arrives, and can be queried by a `RalfClient`. 
# 
# To do so, we'll do the following: 
# 1. Create feature tables from a synthetic stream of user movie rating
# 2. Create a ralf client which queries the feature tables 
# 3. Implement load shedding policies to reduce feature computation cost

# In[15]:


# !pip uninstall -y ralf && pip install git+https://github.com/feature-store/ralf.git@api-for-tutorial
#list the current work dir
#import os
#change the current work dir
#print(os.getcwd())
#os.chdir(os.getcwd())
#print(os.getcwd())
#get_ipython().system('pip uninstall -y ralf && pip install -e ../ralf')
#get_ipython().system('pip list')


# In[17]:


import sys
sys.path

from latency_timing import *

# # Creating a `Ralf` instance 
# We create a instance of ralf to that we can start creating tables for our raw data and features. 

# In[13]:


from ralf import Ralf


# In[ ]:


ralf_server = Ralf()


# ### Creating Source Tables
# Source tables define the raw data sources that are run through ralf to become features. `ralf` lets you create both static batch (e.g. from a CSV) and dynamic streaming sources (e.g. from Kafka). 
# 
# To define a source, we implement a `SourceOperator`. In this example, we'll just synthetically generate data in the `next()` function. 

# In[ ]:


from ralf.operators.source import SourceOperator
from ralf import Record
import random
import time


# In[ ]:


class RatingsSource(SourceOperator):
    def __init__(self, schema):
        super().__init__(schema)

    def next(self):
#         time.sleep(0.01)
        user_id = random.randint(1, 10)
        movie_id = random.randint(100, 200)
        rating = random.randint(1, 5)
        return [Record(user=str(user_id), movie=movie_id, rating=rating)]


# We specify a schema using ralf's `Schema` object. 

# In[ ]:


from ralf import Schema

source_schema = Schema(
    primary_key="user", columns={"user": str, "movie": int, "rating": float}
)


# We can now add the source to our ralf instance. 

# In[ ]:


source = ralf_server.create_source(RatingsSource, args=(source_schema,))


# ### Creating Feature Tables 
# Now that we have data streaming into ralf through the source table, we can define derived feature tables from the source table. 
# 
# Feature tables follow an API similar to pandas dataframes. We define feature tables in terms of 1-2 parent tables and an operator which specifies how to transform parent data. 
# 
# 
# For example, we can calculate the average rating for each user with an `AverageRating` operator: 

# In[ ]:


from collections import defaultdict
import numpy as np

from ralf import Operator, Record


# In[ ]:


class AverageRating(Operator):
    def __init__(self, schema):
        self.user_ratings = defaultdict(list)

        super().__init__(schema)

    def on_record(self, record: Record):
        self.user_ratings[record.user].append(record.rating)
        ratings = np.array(self.user_ratings[record.user])
        output_record = Record(user=record.user, average=ratings.mean())
        return output_record  


# The `AverageRating` operator can be used to define a feature table containing the average rating for each user. 

# In[ ]:


average_rating_schema = Schema(
    primary_key="user", columns={"user": str, "average": float}
)
average_rating = source.map(AverageRating, args=(average_rating_schema,))


# ### Adding Processing Policies
# In many cases, we may only need to sub-sample some of the data to get the features we need. We can add a simple load shedding policy to the `average_rating` table. 

# In[ ]:


from ralf import LoadSheddingPolicy, Record


# In[ ]:


class SampleHalf(LoadSheddingPolicy):
    
    def process(self, candidate_record: Record, current_record: Record) -> bool:
        return random.random() < 0.5

average_rating.add_load_shedding(SampleHalf)


# ## Setting Tables as Queryable 
# Finally, we can set the tables which we want to be queryable by the client with `.as_queryable(table_name)`. We'll set both the `average_rating` and `source` tables as queryable. 

# In[ ]:


average_rating.as_queryable("average")
source.as_queryable("source")


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

print(asyncio.run(timed_table_bulk_query(user_vectors)))

print()