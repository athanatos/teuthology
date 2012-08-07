import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        rados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          max_in_flight: <max number of operations in flight>
          object_size: <size of objects in bytes>
          min_stride_size: <minimum write stride size in bytes>
          max_stride_size: <maximum write stride size in bytes>
          op_weights: <dictionary mapping operation type to integer weight>

    For example::

        tasks:
        - ceph:
        - small_io_bench:
            clients: [client.0]
            args:
               '--num-ops': 1000
        - interactive:
    """
    log.info('Beginning smalliobench')
    args = config.get('args', {});
    arglist = [str(i) for sublist in config.get('args', {}).iteritems() \
                   for i in sublist]
    log.info('args are ' + ' '.join(arglist))
    
    args = [
        'CEPH_CONF=/tmp/cephtest/ceph.conf',
        'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
        '/tmp/cephtest/enable-coredump',
        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
        '/tmp/cephtest/archive/coverage',
        '/tmp/cephtest/binary/usr/local/bin/smalliobench',
        ]
    args += arglist
    
    tests = {}
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        remote.run(
            args=['sudo', 'apt-get', 'install', '-y',
                  'libboost-program-options1.46.1'],
            logger=log.getChild('smalliobench.{id}'.format(id=id_))
            )
        proc = remote.run(
            args=(args + ["--ceph-client-id=%s"%(str(id_),)]),
            stdin=run.PIPE,
            stdout=log.getChild('smalliobench.{id}'.format(id=id_)).getChild('out'),
            stderr=None,
            wait=False
            )
        tests[id_] = proc

    try:
        yield
    finally:
        log.info('joining small_io_bench')
        run.wait(tests.itervalues())

        
