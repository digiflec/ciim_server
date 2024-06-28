from datetime import datetime, timedelta
import threading
from typing import List, Dict
import json
import time
import logging
import random

import asyncio
from datetime import datetime
from typing import List, Dict


logging.basicConfig(level=logging.INFO)

class Cache:
    def __init__(self):
        self.data = []
        self.lock = threading.Lock()

    def add(self, entry: Dict[str, str]) -> None:
        entry['formatted_time'] = datetime.fromisoformat(entry['formatted_time'])
        with self.lock:
            if self.data and entry['formatted_time'] < self.data[-1]['formatted_time']:
                raise ValueError("New entry's time is older than the latest entry in the cache")
            self.data.append(entry)
            # logging.info(f"Added: {entry}")

    def get_all(self) -> List[Dict[str, str]]:
        with self.lock:
            return list(self.data)

    def pop_first(self) -> Dict[str, str]:
        with self.lock:
            if not self.data:
                return None
            return self.data.pop(0)


    def find_closest_index(self, timestamp: datetime) -> int:
        target_time = timestamp
        closest_index = None
        closest_diff = float('inf')

        with self.lock:
            # print('looking for closest index')
            for i, entry in enumerate(self.data):
                time_diff = abs((entry['formatted_time'] - target_time).total_seconds())
                # print('time diff is: ', time_diff)
                if time_diff < closest_diff:
                    closest_diff = time_diff
                    closest_index = i
                else:
                    break
        return closest_index

    def slice_left(self, index: int) -> List[Dict[str, str]]:
        with self.lock:
            if index < 0 or index >= len(self.data):
                raise IndexError("Index out of range")
            left_slice = self.data[:index + 1]
            self.data = self.data[index + 1:]
            return left_slice

    def save_to_json(self, filename: str) -> None:
        
        with open(filename, 'w') as f:
            data_to_save = []
            for entry in self.data:
                entry_copy = entry.copy()
                entry_copy['formatted_time'] = entry_copy['formatted_time'].isoformat()
                data_to_save.append(entry_copy)

            json.dump(data_to_save, f, indent=4)


class SynchronizationManagerOld:
    def __init__(self, base_cache: Cache, caches: List[Cache]):
        self.base_cache = base_cache
        self.caches = caches
        self.synced_data = []  # List to store synchronized data
        self.new_data_event = asyncio.Event()
        self.stop_event = asyncio.Event()

    async def synchronize_and_process(self) -> None:
        while not self.stop_event.is_set():
            print('new_data_event event: ', self.new_data_event.is_set())
            await self.new_data_event.wait()
            print('new_data_event event after await: ', self.new_data_event.is_set())
            self.new_data_event.clear()
            
            if self.stop_event.is_set():
                break
            entry = self.base_cache.pop_first()
            if not entry:
                continue
            entry_time = entry['formatted_time']
            print('sync processing for ', entry_time)
            await asyncio.gather(*(self.wait_for_data(cache, entry_time) for cache in self.caches[1:]))
            
            closest_entries = self.process_entries(entry_time)
            self.validate_time_diff(entry, closest_entries)
            synced_entry = {f'cache{i}': closest_entries[i-1] for i in range(1, len(closest_entries) + 1)}
            synced_entry['cache0'] = entry
            self.synced_data.append(synced_entry)
            print("Synchronized entries:", [entry['formatted_time'] for entry in closest_entries])

    async def wait_for_data(self, cache, entry_time):
        while True:
            cache_data = cache.get_all()
            if not cache_data or cache_data[-1]['formatted_time'] < entry_time:
                await asyncio.sleep(0.05)  # Adjusting for 20 Hz data rate
                continue
            break

    def process_entries(self, base_entry_time: datetime) -> List[Dict[str, str]]:
        closest_entries = []
        for cache in self.caches:
            closest_index = cache.find_closest_index(base_entry_time)
            closest_entry = cache.slice_left(closest_index)[-1]  # Get the closest entry and slice the cache
            closest_entries.append(closest_entry)
        return closest_entries

    def validate_time_diff(self, base_entry: Dict[str, str], closest_entries: List[Dict[str, str]]) -> None:
        base_time = base_entry['formatted_time']
        for entry in closest_entries:
            entry_time = entry['formatted_time']
            time_diff = abs((base_time - entry_time).total_seconds())
            if time_diff > 0.2:
                raise ValueError(f"Time difference between base entry and synced entry exceeds 0.2 seconds: {time_diff:.3f} seconds")

    def start_synchronization(self) -> None:
        asyncio.create_task(self.synchronize_and_process())

    def stop(self) -> None:
        self.stop_event.set()
        self.new_data_event.set()  # Ensure the wait is exited immediately

    def new_data_available(self) -> None:
        print('setting new data available event')
        self.new_data_event.set()
        print('in function: new data set: ', self.new_data_event.is_set())

class SynchronizationManager:
    def __init__(self, base_cache: Cache, caches: List[Cache]):
        self.base_cache = base_cache
        self.caches = caches
        self.synced_data = []  # List to store synchronized data
        self.new_data_event = asyncio.Event()
        self.stop_event = asyncio.Event()

    async def synchronize_and_process(self) -> None:
        while not self.stop_event.is_set():
            
            if self.new_data_event.is_set():
                # print('New data event triggered!')
                self.new_data_event.clear()
                if self.stop_event.is_set():
                    print('stop event is set')
                    break
                # print('after stop')
                entry = self.base_cache.pop_first()
                # print(entry)
                if not entry:
                    print('no entry, continue')
                    await asyncio.sleep(0.05)
                    continue
                
                entry_time = entry['formatted_time']
                # print('Processing entry for time:', entry_time)
                for cache in self.caches[1:]:
                    self.wait_for_data(cache, entry_time)
                
                closest_entries = self.process_entries(entry_time)
                self.validate_time_diff(entry, closest_entries)
                synced_entry = {f'cache{i}': closest_entries[i-1] for i in range(1, len(closest_entries) + 1)}
                synced_entry['cache0'] = entry
                self.synced_data.append(synced_entry)
                print("Synchronized entries:", [entry['formatted_time'] for entry in synced_entry.values()])
                # await asyncio.sleep(0.05)
    def wait_for_data(self, cache, entry_time):
        while True:
            cache_data = cache.get_all()
            if not cache_data or cache_data[-1]['formatted_time'] < entry_time:
                
                    # print('cache latest time: ', cache_data[-1]['formatted_time'])
                
                # time.sleep(0.05)  # Adjusting for 20 Hz data rate
                continue
            # print('found data in other cache')
            # print('cache latest time: ', cache_data[-1]['formatted_time'])
            break

    def process_entries(self, base_entry_time: datetime) -> List[Dict[str, str]]:
        closest_entries = []
        for cache in self.caches[1:]:
            closest_index = cache.find_closest_index(base_entry_time)
            closest_entry = cache.slice_left(closest_index)[-1]  # Get the closest entry and slice the cache
            # print('found closest entry:', closest_entry)
            closest_entries.append(closest_entry)
        return closest_entries

    def validate_time_diff(self, base_entry: Dict[str, str], closest_entries: List[Dict[str, str]]) -> None:
        base_time = base_entry['formatted_time']
        for entry in closest_entries:
            entry_time = entry['formatted_time']
            time_diff = abs((base_time - entry_time).total_seconds())
            if time_diff > 0.2:
                raise ValueError(f"Time difference between base entry and synced entry exceeds 0.2 seconds: {time_diff:.3f} seconds")

    async def start_synchronization(self) -> None:
        print("Started synchronization coroutine")
        await self.synchronize_and_process()
        


    def stop(self) -> None:
        self.stop_event.set()
        self.new_data_event.set()  # Ensure the wait is exited immediately
        self.save_synced_data_to_json("synced_data.json")

    def new_data_available(self) -> None:
        
        # print('New data available, setting event.')
        self.new_data_event.set()
        # print('In function: new data set:', self.new_data_event.is_set())

    def save_synced_data_to_json(self, filename: str) -> None:
            with open(filename, 'w') as f:
                json.dump(self.synced_data, f, indent=4, default=str)
            print(f"Synced data saved to {filename}")

def process_generators(caches: List[Cache], generators: List[callable]) -> List[threading.Thread]:
    def worker(cache: Cache, generator: callable) -> None:
        for entry in generator:
            cache.add(entry)
            time.sleep(0.1)  # Simulate real-time processing delay

    threads = []
    for cache, generator in zip(caches, generators):
        thread = threading.Thread(target=worker, args=(cache, generator))
        threads.append(thread)
        thread.start()

    return threads

def continuous_generator():
    base_time = datetime(2024, 5, 30, 10, 0)
    while True:
        increment_seconds = random.randint(1, 10)
        base_time += timedelta(seconds=increment_seconds)
        data = {
            'formatted_time': base_time.isoformat(),
            'data': f"Data-{random.randint(1, 100)}"
        }
        yield data
        time.sleep(1)  # Simulate delay between data entries


# # Initialize caches and generators
# caches = [Cache() for _ in range(3)]
# generators = [continuous_generator() for _ in range(3)]

# # Process generators and populate caches
# threads = process_generators(caches, generators)

# # Let the generators run for a bit to populate the caches
# time.sleep(5)

# # Initialize and start synchronization manager
# synchronization_manager = SynchronizationManager(caches[0], caches)
# synchronization_manager.start_synchronization()

# # Continue running the main program
# try:
#     while True:
#         time.sleep(1)  # Keep the main thread alive to allow background threads to run
# except KeyboardInterrupt:
#     logging.info("Program terminated")
