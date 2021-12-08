from typing import Dict, Hashable, List
from enum import Enum
from ralf import Record
from .policies import LRU, MRU, Hyperbolic, Random
from .disk import Disk

EVICTION_POLICIES = dict(
        lru=LRU,
        mru=MRU,
        hyperbolic=Hyperbolic,
        random=Random,
        efficient_lru=LRU,
        efficient_mru=MRU,
        efficient_hyperbolic=Hyperbolic,
        efficient_random=Random
    )

ONE_KILOBYTE = 1e3
ONE_MEGABYTE = 1e6
ONE_GIGABYTE = 1e9

DEFAULT_MEMORY_CAPACITY_NUM_BYTES = ONE_KILOBYTE #10*ONE_MEGABYTE

DEFAULT_MEMORY_ACCESS_READING_LATENCY_SEC = 2
DEFAULT_MEMORY_ACCESS_WRITING_LATENCY_SEC = 4
DEFAULT_DISK_ACCESS_READING_LATENCY_SEC = DEFAULT_MEMORY_ACCESS_READING_LATENCY_SEC*359
DEFAULT_DISK_ACCESS_WRITING_LATENCY_SEC = DEFAULT_DISK_ACCESS_READING_LATENCY_SEC*2

COST_AWARE_OPTIMIZATION_ENABLED = True
DEFAULT_RECORD_NUM_BYTES = 12

class MemoryManager:
    def __init__(self, 
        memory_capacity_num_bytes=DEFAULT_MEMORY_CAPACITY_NUM_BYTES,
        memory_access_reading_latency_sec=DEFAULT_MEMORY_ACCESS_READING_LATENCY_SEC,
        memory_access_writing_latency_sec=DEFAULT_MEMORY_ACCESS_WRITING_LATENCY_SEC,
        disk_access_reading_latency_sec=DEFAULT_DISK_ACCESS_READING_LATENCY_SEC,
        disk_access_writing_latency_sec=DEFAULT_DISK_ACCESS_WRITING_LATENCY_SEC,
        eviction_policy="lru",
        is_cost_aware_optimization_enabled=COST_AWARE_OPTIMIZATION_ENABLED,
        record_num_bytes=DEFAULT_RECORD_NUM_BYTES
    ):
        self.memory_capacity_num_bytes = memory_capacity_num_bytes
        self.memory_access_reading_latency_sec = memory_access_reading_latency_sec
        self.memory_access_writing_latency_sec = memory_access_writing_latency_sec
        self.disk_access_reading_latency_sec = disk_access_reading_latency_sec
        self.disk_access_writing_latency_sec = disk_access_writing_latency_sec
        self.is_cost_aware_optimization_enabled = is_cost_aware_optimization_enabled
        self.record_num_bytes = record_num_bytes

        # Stores records in memory
        self.mem_cache: Dict[Hashable, Record] = dict()
    
        self.disk = Disk()

        self.eviction_policy = EVICTION_POLICIES.get(eviction_policy, LRU)(self.memory_capacity_num_bytes)
        self.eviction_policy_str = eviction_policy
    
    def get(self, key: Hashable, opt_params={}) -> 'tuple[Record, bool]':
        did_fetch_from_disk = False
        eviction_candidates = []

        if key in self.mem_cache:
            record = self.mem_cache[key]
            return record, did_fetch_from_disk, eviction_candidates
        
        did_fetch_from_disk = True
        record = self.disk.fetch(key)
        if record is None:
            return None, did_fetch_from_disk, eviction_candidates

        self.mem_cache[key] = record
        # print(f"Getting key: {key} from mem cache")
        eviction_candidates = self.eviction_policy.decide_eviction_candidates(key, self.get_record_num_bytes())

        return record, did_fetch_from_disk, eviction_candidates

    def set(self, key: Hashable, record: Record):
        self.mem_cache[key] = record
        # print(f"Setting key: {key} into mem cache")

        # Cache might be full so decide and evict candidates
        eviction_candidates = self.eviction_policy.decide_eviction_candidates(key, self.get_record_num_bytes())
        
        return eviction_candidates
        
    def toggle_cost_aware_optimization_enabled_parameter(self, isEnabled):
        self.is_cost_aware_optimization_enabled = isEnabled
    
    def set_record_num_bytes(self, record_num_bytes):
        self.record_num_bytes = record_num_bytes
    
    def get_record_num_bytes(self):
        return self.record_num_bytes

    def evict(self, eviction_candidates: List['tuple[Hashable, int]'], opt_params={}):
        # print(f"Eviction candidates: {eviction_candidates}")
        for key in eviction_candidates:
            record = self.mem_cache[key]
            del self.mem_cache[key]
            # print(f"Removing key: {key} from mem cache")

            op_latency = opt_params.get('op_latency', None)

            # TODO(shreyas): Will need to change op_latency and tweak optimization condition            
            if self.is_cost_aware_optimization_enabled and ((op_latency is not None and op_latency < self.disk_access_reading_latency_sec + self.disk_access_writing_latency_sec)):
                # Not a raw feature so we can optimize by recalculating if record op latency < cost to read and write record from disk
                continue
            
            self.disk.save(key, record)

        return
