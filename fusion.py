import signal

from kinesis_stream import KinesisStream
from sync import *
from utils import *

kinesis_stream1 = KinesisStream('museum-outsight-1', create_if_not_found=False)
kinesis_stream2 = KinesisStream('museum-outsight-2', create_if_not_found=False)


records_gen1 = kinesis_stream1.get_records_iter(shard_iter_type="LATEST") #TRIM_HORIZON
records_gen2 = kinesis_stream2.get_records_iter(shard_iter_type="LATEST")

caches = [Cache() for _ in range(2)]
# generators = [continuous_generator() for _ in range(3)]
generators = [records_gen1, records_gen2]

synchronization_manager = SynchronizationManager(caches[0], caches)


def process_generators(caches, generators):
    def base_worker(cache, generator):
        for entry in generator:
            entry = record_to_frame(entry)
            if entry is None:
                continue
            cache.add(entry)
            synchronization_manager.new_data_available()
            
    def worker(cache, generator):
        for entry in generator:
            entry = record_to_frame(entry)
            if entry is None:
                continue
            cache.add(entry)

    threads = []

    thread = threading.Thread(target=base_worker, args=(caches[0], generators[0]))
    threads.append(thread)
    thread.start()
    
    for cache, generator in zip(caches[1:], generators[1:]):
        thread = threading.Thread(target=worker, args=(cache, generator))
        threads.append(thread)
        thread.start()

    return threads




async def fusion():
    try:
        # Run the synchronization process
        await synchronization_manager.start_synchronization()
    except asyncio.CancelledError:
        print("Synchronization cancelled")
    
    synchronization_manager.stop()
    await asyncio.sleep(1)  # Ensure sync_manager stops properly

    print(f"Total synchronized entries: {len(synchronization_manager.synced_data)}")



def main():
    threads = process_generators(caches, generators)

    loop = asyncio.get_event_loop()

    # Function to handle keyboard interrupt and stop synchronization
    def handle_interrupt(signal, frame):
        print("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...")
        synchronization_manager.stop()
        for thread in threads:
            thread.join()
        loop.call_soon_threadsafe(loop.stop)


    # Register the signal handler
    signal.signal(signal.SIGINT, handle_interrupt)

    try:
        loop.run_until_complete(fusion())
    except KeyboardInterrupt:
        print("Keyboard Interrupt. Stopping synchronization.")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == '__main__':
    main()
