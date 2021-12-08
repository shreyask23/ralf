from typing import List
from abc import ABCMeta, abstractmethod
import heapq
import random

class RobustHeap:

    def __init__(self, lst: List):
        self.heap = lst
        self.active = dict()
        self.dummy_value = ''
    
    def heappush(self, priority, item):
        new_heap_entry = [priority, item]
        if item in self.active:
            self.active[item][1] = self.dummy_value
        self.active[item] = new_heap_entry
        # print('new heap entry', new_heap_entry)
        # print('heap', self.heap)
        heapq.heappush(self.heap, new_heap_entry)
    
    def heappop(self):
        if len(self.heap) == 0:
            return None
        entry = [None, self.dummy_value]
        while entry[1] is self.dummy_value:
            entry = heapq.heappop(self.heap)
            if entry[1] is not self.dummy_value:
                del self.active[entry[1]]
        return entry[1]
    
    def heappeak(self):
        if len(self.heap) == 0:
            return None
        entry = self.heap[0]
        while entry[1] is self.dummy_value:
            heapq.heappop(self.heap)
            entry = self.heap[0]
        return entry[1]


class FlatEvictionPolicy:

    def __init__(self, capacity):
        self.capacity = capacity
        self.h = []
        self.rh = RobustHeap(self.h)
        self.key_dict = dict()

    def calculate_priority(self, key=None):
        return 1
    
    def decide_eviction_candidates(self, key, size) -> List:
        # print("Deciding eviction candidates based on key: ", key)
        # print("Current key_dict:", self.key_dict)
        if key in self.key_dict:
            self.capacity += self.key_dict[key]

        self.capacity -= size
        self.key_dict[key] = size

        self.rh.heappush(self.calculate_priority(key), key)
        
        records_to_evict = []
        while self.capacity < 0:
            next_record = self.rh.heappop()
            self.capacity += self.key_dict[next_record]
            del self.key_dict[next_record]
            records_to_evict.append(next_record)

        return records_to_evict


class LRU(FlatEvictionPolicy):

    def __init__(self, capacity):
        super().__init__(capacity)
        self.next_priority = 0
    
    def calculate_priority(self, key=None):
        priority = self.next_priority
        self.next_priority += 1
        return priority


class MRU(FlatEvictionPolicy):

    def __init__(self, capacity):
        super().__init__(capacity)
        self.next_priority = 0
    
    def calculate_priority(self, key=None):
        priority = self.next_priority
        self.next_priority -= 1
        return priority


class Hyperbolic(FlatEvictionPolicy):

    def __init__(self, capacity):
        super().__init__(capacity)
        self.frequency_dict = dict()
        self.time_dict = dict()
        self.timestamp = 0
    
    def calculate_priority_value(self, key=None):
        t_delta = self.time_dict[key] - self.timestamp

        priority = None
        if t_delta < 1:
            priority = -float("inf")
        else:
            priority = -self.frequency_dict[key] / t_delta

        return priority
    
    def calculate_priority(self, key=None):
        self.timestamp += 1
        self.frequency_dict[key] = self.frequency_dict.get(key, 0) + 1
        if key not in self.time_dict:
            self.time_dict[key] = self.timestamp

        return self.calculate_priority_value(key)
    
    def decide_eviction_candidates(self, key, size) -> List:
        eviction_keys = super().decide_eviction_candidates(key, size)
        # print("heap: ", self.h)
        for ek in eviction_keys:
            del self.frequency_dict[ek]
            del self.time_dict[ek]
        for key in self.time_dict.keys():
            self.rh.heappush(self.calculate_priority_value(key), key)
        return eviction_keys

class Random(FlatEvictionPolicy):

    def __init__(self, capacity):
        super().__init__(capacity)
        self.next_priority = random.choice(range(20))
    
    def calculate_priority(self, key=None):
        return random.choice(range(20))
