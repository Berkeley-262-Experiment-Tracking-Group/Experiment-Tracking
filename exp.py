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
import argparse
import os, os.path
import shutil
import hashlib
import time
import re

RESULTS_PATH = 'results'
DESCR_FILE = 'descr'

# very simple serialization: dict.__repr__
# human readable, no dependencies
def save_descr(path, info):
    """Save info about an experiment to a file

    info is intended to be a dictionary of objects for which repr does the
    right thing"""

    f = open(path, 'w')
    f.write(repr(info))
    f.close()

def load_descr(path):
    """Load info about an experiment as saved by save_descr"""

    f = open(path)
    info = eval(f.read())
    f.close()
    return info

def exec_cmd(args):
    p = subprocess.Popen(args)
    return os.waitpid(p.pid, 0)[1]

def exec_shell(cmd):
    p = subprocess.Popen(cmd, shell=True)
    return os.waitpid(p.pid, 0)[1]

def exec_output(args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

def trunc(s, n):
    if len(s) <= n:
        return s
    else:
        return s[:n] + '...'

def sha1(s):
    return hashlib.sha1(s).hexdigest()

def abs_root_path():
    return exec_output(['git', 'rev-parse', '--show-toplevel']).strip()

def handle_existing(exp_dir):
    resultsdir = os.path.join(abs_root_path(), RESULTS_PATH, exp_dir)
    if os.path.isdir(resultsdir):
        try:
            date = load_descr(os.path.join(resultsdir, DESCR_FILE))['date']
        except Exception as e:
            print ('A results directory already exists for this experiment, but\n'
                   ' the description cannot be read. This indicates a problem with\n'
                   ' this script that must be fixed to continue.')
            print e
            return True

        print ('This experiment (' + trunc(exp_dir, 6) + ') appears'
               ' to have already been run\n at ' + time.ctime(date))
        return True
    return False

def find_by_descr(exps, descr):
    exps = read_descrs()
    matches = [exp_hsh for exp_hsh in exps.iterkeys()
                    if exps[exp_hsh]['description'] == descr]


    # note: descending sort
    matches.sort(lambda x, y: cmp(exps[y]['date'], exps[x]['date']))

    return matches

def expand_command(cmd):
    """Replace special sequences in cmd with appropriate paths specifying
    output directory and input from other experiments

    Note that the experimental hash itself is not inserted here, since it
    must be computed from the output of this function"""

    exps = read_descrs()

    deps = []

    def expander(m):
        if m.group(1) == '':
            # let this be handled in the next pass
            return '{}'
        else:
            exp_hshs = find_by_descr(exps, m.group(1))

            if len(exp_hshs) == 0:
                print 'Error: could not match %s' % m.group(1)
                exit(1)

            if len(exp_hshs) > 1:
                print 'Warning: found multiple matches for %s' % m.group(1)
                print 'Using latest (%s)' % time.ctime(exps[exp_hshs[0]]['date'])

            deps.append(exp_hshs[0])
            return os.path.join(abs_root_path(), RESULTS_PATH, exp_hshs[0])

    return (re.sub('{(.*?)}', expander, cmd), deps)

def run_exp(args):
    # switch to this branch
    sts = exec_cmd(['git', 'checkout', '-f', args.commit])
    if sts != 0:
        print 'Attempt to switch branches failed with error ' + str(sts)
        sys.exit(1)

    # find out the hash of experimental commit
    hsh = exec_output(['git', 'rev-parse', 'HEAD']).strip()

    # compute a hash unique to this experiment
    exp_hsh = sha1(hsh + args.command)

    # command expansion must be done in two phases:
    #  first, inputs are expanded and indentified
    #  then, the experimental hash is computed and substituted

    # lookup and insert necessary inputs
    new_cmd, deps = expand_command(args.command)

    rootdir = abs_root_path()
    resultsdir = os.path.join(rootdir, RESULTS_PATH)
    working_dir = os.path.relpath(os.getcwd(), rootdir)

    # compute a hash unique to this experiment
    # this encoding is not totally fool-proof; maybe look to git
    #  for ideas?
    exp_hsh = sha1(hsh + ''.join(deps) + str(len(working_dir)) +
                   working_dir + str(len(args.command)) + args.command)

    exp_path = os.path.join(resultsdir, exp_hsh)

    # save to a log file
    new_cmd = new_cmd + ' | tee {}/log 2>&1'

    # substitute in cmd where necessary
    new_cmd = new_cmd.replace('{}', exp_path)

    # create the results directory if necessary
    if not os.path.isdir(resultsdir):
        os.mkdir(resultsdir)

    # check if this experiment has been run
    exp_path = os.path.join(resultsdir, exp_hsh)
    if handle_existing(exp_hsh):
        sys.exit(1)

    # the experimental description
    info = {'commit': hsh, 'command': args.command, 'date': time.time(),
            'description': args.description,
            'working_dir': working_dir, 'deps': deps}

    # import existing results if requested
    # awkward name conflict here
    if vars(args)['import']:
        exps = read_descrs()
        matches = find_by_descr(exps, args.description)

        if len(matches) == 0:
            print 'Could not find matching experiment to import from'
            exit(1)

        info['import'] = matches[0]
        import_dir = os.path.join(abs_root_path(), RESULTS_PATH, matches[0])

        shutil.copytree(import_dir, exp_path)
    else:
        # create the results directory for this experiment
        # (should only do this once we know we can at least write info file)
        os.mkdir(exp_path)

    # save the experimental description
    save_descr(os.path.join(exp_path, DESCR_FILE), info)

    # run the experiment
    print 'Running command ' + new_cmd
    sts = exec_shell(new_cmd)
    print 'Command exited with status ' + str(sts)
    info['exit_status'] = sts
    info['date_end'] = time.time()
    save_descr(os.path.join(exp_path, DESCR_FILE), info)
    
def read_descrs():
    resultsdir = os.path.join(abs_root_path(), RESULTS_PATH)
    exps = {}
    exp_dirs = os.listdir(resultsdir)
    for exp_dir in exp_dirs:
        try:
            exps[exp_dir] = load_descr(os.path.join(resultsdir,
                                                    exp_dir, DESCR_FILE))
        except Exception as e:
            print 'Error reading description for ' + exp_dir
            print e
            sys.exit(1)
    return exps

def list_descrs(exps):
    for (exp_hsh, info) in exps.iteritems():
        print (trunc(exp_hsh, 6) + '\t'
               + trunc(info['commit'], 6) + '\t'
               + trunc(info['command'], 20) + '\t'
               + time.ctime(info['date']) + '\t'
               + trunc(info['description'], 30))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Track content created by code')
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser('run', help='run an experiment')
    run_parser.add_argument('--import', action='store_true', help='import results from last run of this experiment')
    run_parser.add_argument('commit', help='git commit expression indicating code to run')
    run_parser.add_argument('command', help='command to run')
    run_parser.add_argument('description', help='unique description of this experiment')
    run_parser.set_defaults(func=run_exp)

    list_parser = subparsers.add_parser('list', help='list previous experiments')
    list_parser.set_defaults(func=lambda args: list_descrs(read_descrs()))

    args = parser.parse_args()
    args.func(args)
