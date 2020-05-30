from memc_load import ProcessedLine


def test_thread_processed(device_memc, sample_queue):
    size_queue = sample_queue.qsize()
    sample_queue.put('quit')
    thread = ProcessedLine(device_memc, sample_queue)
    thread.start()
    thread.join()
    assert sum((thread.errors, thread.processed)) == size_queue


def test_pool_thread_processed(device_memc, sample_queue):
    size_queue = sample_queue.qsize()
    sample_queue.put('quit')
    sample_queue.put('quit')
    thread = ProcessedLine(device_memc, sample_queue)
    thread2 = ProcessedLine(device_memc, sample_queue)
    thread.start()
    thread2.start()
    thread.join()
    thread2.join()
    assert sum((thread.errors, thread.processed, thread2.errors, thread2.processed)) == size_queue
