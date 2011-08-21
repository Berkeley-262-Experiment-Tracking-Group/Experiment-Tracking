#!/usr/bin/env python

# exp.py: code for managing experiments from a git repo such that:
#  - experiments are reproducible: it is always possible to see exactly which
#    code certain results came from
#  - it is possible to test whether an experiment has already been performed
#    and find the corresponding results
#  - experiments using the same code can be run in parallel
# Other niceties may be added as useful.
#
# NOTE: this code should not be run on a development copy of a repo, as working
#  files may be overwritten at any time. A separate experimental copy is
#  suggested.

import subprocess
import sys
import os, os.path
import hashlib
import time

RESULTS_PATH = 'results'
DESCR_FILE = 'descr'

def exec_cmd(args):
    p = subprocess.Popen(args)
    return os.waitpid(p.pid, 0)[1]

def exec_shell(cmd):
    p = subprocess.Popen(cmd, shell=True)
    return os.waitpid(p.pid, 0)[1]

def exec_output(args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

def sha1(s):
    return hashlib.sha1(s).hexdigest()

def read_descr(exp_dir):
    f = open(os.path.join(RESULTS_PATH, exp_dir, DESCR_FILE), 'r')
    hsh = f.readline().strip()
    cmd = f.readline().strip()
    date = f.readline().strip()
    f.readline()
    descr = f.readline().strip()
    f.close()
    return (hsh, cmd, date, descr)

def handle_existing(exp_dir):
    if os.path.isdir(os.path.join(RESULTS_PATH, exp_dir)):
        try:
            date = read_descr(exp_dir)[2]
        except:
            print ('A results directory already exists for this experiment, but\n'
                   ' the description cannot be read. This indicates a problem with\n'
                   ' this script that must be fixed to continue.')
            return True

        print 'This experiment appears to have already been run at ' + date
        return True
    return False


def run_exp(branch, cmd, descr):
    # switch to this branch
    sts = exec_cmd(['git', 'checkout', '-f', branch])
    if sts != 0:
        print 'Attempt to switch branches failed with error ' + str(sts)
        sys.exit(1)

    # find out the hash of experimental commit
    hsh = exec_output(['git', 'rev-parse', 'HEAD']).strip()

    # compute a hash unique to this experiment
    exp_hsh = sha1(hsh + cmd)

    # create the results directory if necessary
    if not os.path.isdir(RESULTS_PATH):
        os.mkdir(RESULTS_PATH)

    # check if this experiment has been run
    exp_path = os.path.join(RESULTS_PATH, exp_hsh)
    if handle_existing(exp_hsh):
        sys.exit(1)

    # create the results directory for this experiment
    os.mkdir(exp_path)

    # save the experimental description
    f = open(os.path.join(RESULTS_PATH, exp_hsh, DESCR_FILE), 'w')
    f.write(hsh + '\n')
    f.write(cmd + '\n')
    f.write(time.ctime() + '\n')
    f.write('\n')
    f.write(descr)
    f.close()

    # run the experiment
    new_cmd = cmd.replace('{}', exp_path)
    print 'Running command ' + new_cmd
    sts = exec_shell(new_cmd)
    print 'Command exited with status ' + str(sts)
    
def read_descrs():
    exps = {}
    exp_dirs = os.listdir(RESULTS_PATH)
    for exp_dir in exp_dirs:
        try:
            exps[exp_dir] = read_descr(exp_dir)
        except:
            print 'Error reading description for ' + exp_dir
            sys.exit(1)
    return exps

def trunc(s, n):
    if len(s) <= n:
        return s
    else:
        return s[:n] + '...'

def list_descrs(exps):
    for (exp_hsh, (hsh, cmd, date, descr)) in exps.iteritems():
        print (trunc(exp_hsh, 6) + '\t'
               + trunc(hsh, 6) + '\t'
               + trunc(cmd, 20) + '\t'
               + date + '\t'
               + trunc(descr, 30))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print ('Missing command. Currently supported:\n'
               '\trun\t Run an experiment\n'
               '\tlist\t List available results\n')
        sys.exit(1)

    if sys.argv[1] == 'run':
        if len(sys.argv) != 5:
            print 'Invalid arguments: ./exp.py run branch cmd descr'
            sys.exit(1)

        run_exp(*sys.argv[2:])

    if sys.argv[1] == 'list':
        list_descrs(read_descrs())
