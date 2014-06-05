"""
Try to reproduce 8532
"""
import contextlib
import logging
from ceph_manager import CephManager
from teuthology import misc as teuthology
import time
import random

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Attempt to reproduce the conditions of 8538
    """
    if config is None:
        config = {}
    client = config.get("client", "client.0")

    (remote,) = ctx.cluster.only(client).remotes.iterkeys()

    if not hasattr(ctx, 'manager'):
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
        ctx.manager = CephManager(
            mon,
            ctx=ctx,
            logger=log.getChild('ceph_manager'),
            )

    ctx.manager.wait_for_clean()
    
    osd_status = ctx.manager.get_osd_status()
    osds = osd_status['in']

    for j in range(20):
        log.info("run %s" % (j,))
        random.shuffle(osds)

        log.info("Killing all osds")
        [ctx.manager.kill_osd(i) for i in osds]
        [ctx.manager.mark_down_osd(i) for i in osds]

        log.info("Waiting 10s")
        time.sleep(10)

        def split_at(x, l):
            return l[:x], l[x:]
        to_revive, still_dead = split_at(2, osds)

        log.info("Reviving the first 2 %s" % (to_revive,))
        [ctx.manager.revive_osd(i) for i in to_revive]

        log.info("Waiting 10s")
        time.sleep(10)

        log.info("Reviving the rest")
        [ctx.manager.revive_osd(i) for i in still_dead]

    log.info("Waiting for clean")
    ctx.manager.wait_for_clean()

    try:
        yield
    finally:
        log.info('done')
