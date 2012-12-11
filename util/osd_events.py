#!/usr/bin/env python
import json
from datetime import datetime
from datetime import timedelta

class OSDEvt:
    def __init__(self, opid, start):
        self.id = opid
        self.events = []
        self.traits = {}
        self.start_time = start;

    def latency(self):
        return (self.end_time - self.start_time).total_seconds()


def group_events(events, smoothing=0.01, start=None):
    cur = []
    for evt in events:
        if start is None:
            start = evt.end_time
        cur.append(evt)
        while (len(cur) >= 2 and 
               (cur[-1].end_time - cur[0].end_time).total_seconds() > smoothing):
            cur.pop(0)
        yield ((cur[-1].end_time - start).total_seconds(), cur)


def read_file(filename):
    cons = OSDEvtConsumer()
    with open(filename) as f:
        for i in f.xreadlines():
            cons.consume_line(i)
    return cons
    
def lfilter(f):
    def ret(x):
        for i in x:
            if f(i):
                yield i
    return ret

def lmap(f):
    def ret(x):
        for i in x:
            yield f(i)
    return ret

def avg_latency(evts):
    ret = 0
    total = 0
    for i in (j.latency() for j in evts):
        total += 1
        ret += i
    return ret/total

llatency = lmap(lambda (x, y): (x, avg_latency(y)))
lops = lmap(lambda (x, y): (x, len(y)))

def fst(x):
    return [i for (i,j) in x]
def snd(x):
    return [j for (i,j) in x]

class OSDEvtConsumer:
    def __init__(self):
        self.ops = []
        self.open_ops = {}

    def get_start(self):
        return self.ops[0].end_time

    def consume_line(self, line):
        obj = json.loads(line)
        time = datetime.strptime(
            obj['time'], '%Y-%m-%d %H:%M:%S.%f'
            )
        if obj['type'] == 'start_op':
            new_op = OSDEvt(obj['opnum'], time)
            assert new_op.id not in self.open_ops;
            self.open_ops[new_op.id] = new_op;
        elif obj['type'] == 'end_op':
            assert obj['opnum'] in self.open_ops
            self.open_ops[obj['opnum']].end_time = time
            self.ops.append(self.open_ops[obj['opnum']])
            del self.open_ops[obj['opnum']]
        elif obj['type'] == 'trait':
            assert obj['opnum'] in self.open_ops
            self.open_ops[obj['opnum']].traits[obj['trait']] = obj['val']
        elif obj['type'] == 'event':
            self.open_ops[obj['opnum']].events.append((obj['event'], time,))

    def get_ops(self):
        for i in self.ops:
            yield i

    def get_pushes(self):
        return lfilter(lambda x: x.traits.get("optype", "") == "push")(self.get_ops())

    def get_pulls(self):
        return lfilter(lambda x: x.traits.get("optype", "") == "pull")(self.get_ops())

    def get_client_ops(self):
        return lfilter(lambda x: x.is_client_op())(self.get_ops())

def setx(i):
    return read_file("osd_events.%s.log"%(str(i),))

#filters
def push(x):
    return x.traits.get("optype", "") == "push"

def pull(x):
    return x.traits.get("optype", "") == "pull"

def hprio(x):
    return int(x.traits.get("priority", "0")) > 50

def lprio(x):
    return int(x.traits.get("priority", "0")) <= 50

def cop(x):
    return (
        'desc' in x.traits.keys() and
        x.traits['desc'] >= 7 and
        x.traits['desc'][0:7] == 'osd_op(')

def fand(x):
    return lambda y: sum([i(y) for i in x]) == len(x)

#plotter
def plat(evts, filt, pfunc, smt=20):
    glat = lambda: llatency(group_events(lfilter(filt)(evts.get_ops()), start=evts.get_start(), smoothing=smt))
    times = fst(glat())
    lat = snd(glat())
    pfunc(times, lat)

def pops(evts, filt, pfunc, smt=20):
    gops = lambda: lmap(lambda (x,y) : (x, y/float(smt)))(lops(group_events(lfilter(filt)(evts.get_ops()), start=evts.get_start(), smoothing=smt)))
    times = fst(gops())
    ops = snd(gops())
    pfunc(times, ops)
