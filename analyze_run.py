import os
import os.path
import json

def get_most_recent():
    parent = '/home/samuelj/log_output'
    dirs = [subdir for subdir in os.listdir(parent)]
    dirs.sort()
    return os.path.join(parent,dirs[-1])

def break_down(path):
    with open(os.path.join(path, 'teuthology.log')) as f:
        for line in f.readlines():
            if ':summary:' in line:
                return json.loads(" ".join(line.split()[2:]))

def summarize(path):
    output = {}
    for subdir in os.listdir(path):
        (branch, load) = subdir.split('-')
        output.setdefault(branch, {})[load] = break_down(
            os.path.join(path, subdir))
    return output

def project(summary, load, t, feature):
    print "\t before \t after"
    for branch in summary.keys():
        load_info = summary[branch][load]
        print "%s:\t%s\t%s"%(branch,
                             load_info['before'][t][feature],
                             load_info['after'][t][feature])
