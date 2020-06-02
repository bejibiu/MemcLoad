from queue import Queue

import memcache
import pytest

from memc_load import ParseAppsLogThread


@pytest.fixture
def sample():
    return "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"


@pytest.fixture
def appsinstalled(sample):
    return ParseAppsLogThread.parse_appsinstalled(sample.splitlines().pop())


@pytest.fixture
def empty_queue():
    return Queue()


@pytest.fixture
def sample_queue(sample, empty_queue):
    [empty_queue.put([i]) for i in sample.splitlines()]
    return empty_queue


@pytest.fixture
def parsed_queue(appsinstalled, empty_queue):

    [empty_queue.put([appsinstalled]) for i in range(10)]
    return empty_queue


@pytest.fixture
def device_memc():
    return {
        "idfa": memcache.Client(["127.0.0.1:33013"]),
        "gaid": memcache.Client(["127.0.0.1:33014"]),
        "adid": memcache.Client(["127.0.0.1:33015"]),
        "dvid": memcache.Client(["127.0.0.1:33016"]),
    }
