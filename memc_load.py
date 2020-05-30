import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
from queue import Queue
from threading import Thread

import appsinstalled_pb2
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class ProcessedLine(Thread):
    def __init__(self, device_memc, queue, options=None):
        super().__init__()
        self.queue: Queue = queue
        self.timeout = 3 if not options else options.timeout
        self.device_memc = device_memc
        self.retry = 1 if not options else options.retry
        self.dry = False if not options else options.dry
        self.processed = self.errors = 0

    def run(self):
        while True:
            line = self.queue.get()
            if isinstance(line, str) and line == "quit":
                break
            appsinstalled = ProcessedLine.parse_appsinstalled(line)
            if not appsinstalled:
                self.errors += 1
                continue
            memc = self.device_memc.get(appsinstalled.dev_type)
            if not memc:
                self.errors += 1
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue
            ok = ProcessedLine.insert_appsinstalled(memc, appsinstalled, self.dry, self.timeout, self.retry)
            if ok:
                self.processed += 1
            else:
                self.errors += 1
            self.queue.task_done()

    @staticmethod
    def parse_appsinstalled(line):
        line_parts = line.strip().split("\t")
        if len(line_parts) < 5:
            return
        dev_type, dev_id, lat, lon, raw_apps = line_parts
        if not dev_type or not dev_id:
            return
        try:
            apps = [int(a.strip()) for a in raw_apps.split(",")]
        except ValueError:
            apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
            logging.info("Not all user apps are digits: `%s`" % line)
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            logging.info("Invalid geo coords: `%s`" % line)
        return AppsInstalled(dev_type, dev_id, lat, lon, apps)

    @staticmethod
    def insert_appsinstalled(memc, appsinstalled, dry_run=False, timeout=3, retry_connection=3):
        retry_connection_ = retry_connection
        memc_addr = str(memc.servers[0])
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()
        try:
            if dry_run:
                logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
            else:
                result = memc.set(key, packed)
                while not result and retry_connection_ > 0:
                    logging.info(f"set failed. {retry_connection_} attempts left")
                    result = memc.set(key, packed)
                    retry_connection_ -= 1
                if not retry_connection_:
                    logging.info(f"set failed. for {memc_addr}")
                return result
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
            return False


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def main(options):
    queue = Queue()
    device_memc = {
        "idfa": memcache.Client([options.idfa], socket_timeout=options.timeout),
        "gaid": memcache.Client([options.gaid], socket_timeout=options.timeout),
        "adid": memcache.Client([options.adid], socket_timeout=options.timeout),
        "dvid": memcache.Client([options.dvid], socket_timeout=options.timeout),
    }
    for fn in glob.iglob(options.pattern):
        logging.info('Processing %s' % fn)
        workers = start_workers(device_memc, options, queue)
        fd = gzip.open(fn)
        run_filler_thread(fd, queue)
        for _ in workers:
            queue.put('quit')
        for w in workers:
            w.join()

        errors = sum(w.errors for w in workers)
        processed = sum(w.processed for w in workers)
        if not processed:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


def start_workers(device_memc, options, queue):
    workers = []
    for _ in range(options.num_workers):
        worker = ProcessedLine(device_memc, queue, options)
        worker.start()
        workers.append(worker)
    return workers


def run_filler_thread(fd, queue):
    thread_filler = Thread(target=filler_line, args=(fd, queue), name='filler')
    thread_filler.start()
    thread_filler.join()


def filler_line(fd, queue):
    for line in fd:
        line = line.strip().decode()
        if not line:
            continue
        queue.put(line)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--timeout", action="store", default=3, type="int",
                  help='timeout in seconds for all calls to a server memcached. Defaults to 3 seconds.')
    op.add_option("--retry", action="store", default=3, type="int",
                  help='retry connection to set value to memcached. Defaults to 3 attempts')
    op.add_option("--num-workers", action="store", default=5, type="int")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(threadName)s %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
