import logging
import gevent
import random
import time
import copy

from ..orchestra import run

log = logging.getLogger(__name__)

class ProcThrasher:
    """ Kills and restarts some number of the specified process on the specified
        remote
    """
    def __init__(self, config, remote, *proc_args, **proc_kwargs):
        self.proc_kwargs = proc_kwargs
        self.proc_args = proc_args
        self.config = config
        self.greenlet = None
        self.logger = proc_kwargs.get("logger", log.getChild('proc_thrasher'))
        self.remote = remote

        # config:
        self.num_procs = self.config.get("num_procs", 100)
        self.rest_period = self.config.get("rest_period", 100) # seconds
        self.run_time = self.config.get("run_time", 1000) # seconds

    def log(self, msg):
        self.logger.info(msg)

    def start(self):
        if self.greenlet is not None:
            return
        self.greenlet = gevent.Greenlet(self.loop)
        self.greenlet.start()

    def join(self):
        self.greenlet.join()

    def loop(self):
        time_started = time.time()
        procs = []
        self.log("Starting")
        while time_started + self.run_time > time.time():
            if len(procs) > 0:
                self.log("Killing proc")
                proc = random.choice(procs)
                procs.remove(proc)
                proc.stdin.close()
                self.log("About to wait")
                try:
                    run.wait([proc])
                except:
                    pass
                self.log("Killed proc")
                
            while len(procs) < self.num_procs:
                self.log("Creating proc " + str(len(procs) + 1))
                kwargs = copy.copy(self.proc_kwargs)
                kwargs['args'] = self.proc_kwargs.get('args', lambda:[])()
                self.log("args are " + str(self.proc_args) + " kwargs: " + str(kwargs))
                proc = None
                try:
                    proc = self.remote.run(
                         *self.proc_args,
                         **kwargs)
                except:
                    proc = None
                if proc is not None:
                    procs.append(proc)
            self.log("About to sleep")
            time.sleep(self.rest_period)
            self.log("Just woke")

        run.wait(procs)
