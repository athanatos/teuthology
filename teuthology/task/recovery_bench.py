from cStringIO import StringIO

import contextlib
import gevent
import json
import logging
import random
import time

import ceph_manager
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Benchmark the recovery system.

    Generates objects with smalliobench, runs it normally to get a
    baseline performance measurement, then marks an OSD out and reruns
    to measure performance during recovery.

    The config should be as follows:

    recovery_bench:
        duration: <seconds for each measurement run>
        num_objects: <number of objects>
        io_size: <io size in bytes>
        sequential: true|false
        num-concurrent-ops: <num-concurrent-ops>

    example:

    tasks:
    - ceph:
    - recovery_bench:
        duration: 60
        num_objects: 500
        io_size: 4096
        sequential: true
        num-concurrent-ops: 20
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'recovery_bench task only accepts a dict for configuration'

    log.info('Beginning recovery bench...')

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    while len(manager.get_osd_status()['up']) < num_osds:
        manager.sleep(10)

    bench_proc = RecoveryBencher(
        manager,
        config,
        )
    try:
        yield
    finally:
        log.info('joining recovery bencher')
        bench_proc.do_join()

class RecoveryBencher:
    def __init__(self, manager, config):
        self.ceph_manager = manager
        self.ceph_manager.wait_for_clean()

        osd_status = self.ceph_manager.get_osd_status()
        self.osds = osd_status['up']

        self.config = config
        if self.config is None:
            self.config = dict()

        else:
            def tmp(x):
                print x
            self.log = tmp

        log.info("spawning thread")

        self.thread = gevent.spawn(self.do_bench)

    def do_join(self):
        self.thread.get()

    def do_bench(self):
        duration = self.config.get("duration", 60)
        num_objects = self.config.get("num_objects", 500)
        io_size = self.config.get("io_size", 4096)

        osd = str(random.choice(self.osds))
        (osd_remote,) = self.ceph_manager.ctx.cluster.only('osd.%s' % osd).remotes.iterkeys()

        self.ceph_manager.mark_out_osd(0)
        self.ceph_manager.mark_out_osd(1)
	self.ceph_manager.wait_for_clean()

        self.ceph_manager.osd_socket_stream(0, 'osd_events')
        self.ceph_manager.osd_socket_stream(2, 'osd_events')

        # create the objects
        osd_remote.run(
            args=[
                'env', 'CEPH_CONF=/tmp/cephtest/ceph.conf',
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/smalliobench',
                '--use-prefix', 'recovery_bench',
                '--init-only', '1',
                '--num-objects', str(num_objects),
                '--io-size', str(io_size),
                ],
            wait=True,
        )

        # baseline bench
        log.info('non-recovery (baseline)')
        p = osd_remote.run(
            args=[
                'env', 'CEPH_CONF=/tmp/cephtest/ceph.conf',
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/smalliobench',
                '--use-prefix', 'recovery_bench',
                '--do-not-init', '1',
                '--duration', str(duration),
                '--io-size', str(io_size),
                '--sequential', str(self.config.get('sequential', 'false')),
                '--num-concurrent-ops', str(self.config.get('num-concurrent-ops', 20)),
                '--write-ratio', str(self.config.get('write-ratio', 0.7)),
                ],
            stdout=StringIO(),
            stderr=StringIO(),
            wait=True,
        )
        before = {}
        self.process_samples(p.stderr.getvalue(), before)
        self.process_summary(p.stdout.getvalue(), before)

        self.ceph_manager.mark_in_osd(0)
        self.ceph_manager.mark_in_osd(1)
        time.sleep(5)

        # recovery bench
        log.info('recovery active')
        p = osd_remote.run(
            args=[
                'env', 'CEPH_CONF=/tmp/cephtest/ceph.conf',
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/smalliobench',
                '--use-prefix', 'recovery_bench',
                '--do-not-init', '1',
                '--duration', str(duration),
                '--io-size', str(io_size),
                '--sequential', str(self.config.get('sequential', 'false')),
                '--num-concurrent-ops', str(self.config.get('num-concurrent-ops', 20)),
                '--write-ratio', str(self.config.get('write-ratio', 0.7)),
                ],
            stdout=StringIO(),
            stderr=StringIO(),
            wait=True,
        )
        after = {}
        self.process_samples(p.stderr.getvalue(), after)
        self.process_summary(p.stdout.getvalue(), after)

        self.ceph_manager.raw_cluster_cmd('osd', 'in', osd)
        log.info("summary: %s", json.dumps({
                    'before':before,
                    'after':after}))
                    

    def process_summary(self, input, out):
        if len(input) == 0: return
        line = input.split('\n')[-2]
        print line
        j = json.loads(line)
        for i in ['read', 'write_committed']:
            if i not in j.keys():
                continue
            for k in ['avg_total_iops', 'avg_total_throughput',
                      'avg_total_throughput_mb']:
                log.info("%s-%s: %s", i, k, str(j[i][k]))
                out.setdefault(i, {})[k] = float(j[i][k])
                

    def process_samples(self, input, out):
        lat = {}
        for line in input.split('\n'):
            try:
                sample = json.loads(line)
                samples = lat.setdefault(sample['type'], [])
                samples.append(float(sample['latency']))
            except:
              pass

        for type in lat:
            samples = lat[type]
            samples.sort()

            num = len(samples)

            # median
            if num & 1 == 1: # odd number of samples
                median = samples[num / 2]
            else:
                median = (samples[num / 2] + samples[num / 2 - 1]) / 2

            # 99%
            ninety_nine = samples[int(num * 0.99)]

            log.info("%s: median %f, 99%% %f" % (type, median, ninety_nine))
            out.setdefault(type, {})['median'] = float(median)
            out.setdefault(type, {})['ninety_nine'] = float(ninety_nine)
