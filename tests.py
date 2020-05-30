from memc_load import ProcessedLine, generate_chunk


class TestsThreadClass:
    def test_thread_processed(self, device_memc, sample_queue):
        size_queue = sample_queue.qsize()
        sample_queue.put('quit')
        thread = ProcessedLine(device_memc, sample_queue)
        thread.start()
        thread.join()
        assert sum((thread.errors, thread.processed)) == size_queue

    def test_pool_thread_processed(self, device_memc, sample_queue):
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


class TestsFiller:
    def test_generate_chunk(self, tmpdir):
        num_lines = 100
        chunk_size = 10
        p = tmpdir.mkdir("sub").join("tempfile")
        p.write(b"idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\n" * num_lines)
        with open(p.strpath, 'rb') as f:
            gen = generate_chunk(f, chunk_size=chunk_size)
            lines = [i for i in gen]
        assert len(lines) == num_lines // chunk_size

    def test_generate_chunk_not_in_equal_parts(self, tmpdir):
        num_lines = 15
        chunk_size = 10
        p = tmpdir.mkdir("sub").join("tempfile")
        p.write(b"idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\n" * num_lines)
        with open(p.strpath, 'rb') as f:
            gen = generate_chunk(f, chunk_size=chunk_size)
            lines = [i for i in gen]
        assert len(lines) == num_lines // chunk_size + 1
        assert sum(len(i) for i in lines) == num_lines

    def test_generate_chunk_with_one_lines(self, tmpdir):
        chunk_size = 10
        p = tmpdir.mkdir("sub").join("tempfile")
        p.write(b"idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\n")
        with open(p.strpath, 'rb') as f:
            gen = generate_chunk(f, chunk_size=chunk_size)
            lines = [i for i in gen]
        assert len(lines) == 1
        assert sum(len(i) for i in lines) == 1
