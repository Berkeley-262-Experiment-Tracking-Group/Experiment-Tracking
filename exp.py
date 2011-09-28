#!/usr/bin/env python

# exp.py: code for managing experiments from a git repo such that:
#  - experiments are reproducible: it is always possible to see exactly which
#    code certain results came from
#  - it is possible to test whether an experiment has already been performed
#    and find the corresponding results
#  - experiments using the same code can be run in parallel
# Other niceties may be added as useful.
#

# TODO
#  - parallelization
#  - job queue
#  - automatic jobs
#  - above should be managed separately?
#  - exp repeat for repeating experiments with new code
#  - exp run --rerun for rerunning experiments
#  - exp run --subdir-only to checkout only this directory, not the whole tree
#  - exp purge for deleting old data
#  - exp purge --keep-latest for removing redundant data
#  - what should happen when an experiment fails?
#  - directory sharing for parallel experiments
#  - collapse exp list output a bit
#  - exp hash/exp results
#  - exp show
#  - check on running experiments
#  - don't allow running experiments with broken dependencies
#  - allow running of commands with substition, but no trocking
#  - organize this code

import subprocess
import sys
import argparse
import os, os.path
import shutil
import hashlib
import time
import re

RESULTS_PATH = 'results'
EXP_PATH = 'exp'
DESCR_FILE = 'descr'

class Exp:
    """Store/deal with experimental metadata"""

    _cache = {}

    def __init__(self, hsh):
        self.hsh = hsh

        if hsh in Exp._cache:
            self.info = Exp._cache[hsh].info
        else:
            self.info = load_info(hsh)
            Exp._cache[hsh] = self

    def __getitem__(self, name):
        return self.info[name]

    def __contains__(self, name):
        return name in self.info

    def get(self, name):
        return self.info.get(name)

    def __eq__(self, other):
        return self.hsh == other.hsh

    def success(self):
        return self.info.get('exit_status') == 0

    def failure(self):
        return self.info.get('exit_status', 0) != 0

    def deps(self):
        return [Exp(hsh) for hsh in self['deps']]

    def find_deps(self, name):
        return match(name, self.deps())

    def find_dep(self, name):
        return self.find_deps(name)[0]

    def filename(self, name):
        return os.path.join(abs_root_path(), RESULTS_PATH, self.hsh, name)

    def param(self, name):
        return self['params'][name]

    def broken_deps(self):
        if not self.success():
            return True

        return any(Exp(hsh).broken_deps() for hsh in self['deps'])



# very simple serialization: dict.__repr__
# human readable, no dependencies
def save_descr(path, info):
    """Save info about an experiment to a file

    info is intended to be a dictionary of objects for which repr does the
    right thing"""

    with open(path, 'w') as f:
        f.write(repr(info))
        f.write('\n')

def load_info(hsh):
    """Load info about an experiment as saved by save_descr"""

    with open(os.path.join(abs_root_path(), RESULTS_PATH, hsh, DESCR_FILE)) as f:
        return eval(f.read())

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
        return s[:n] + '..'

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

def match(s, exps):
    exact_match = lambda x, d: x == d
    prefix_match = lambda x, d: x.hsh.startswith(d.hsh)
    exact_descr_match = lambda x, d: x['description'] == d
    prefix_descr_match = lambda x, d: x['description'].startswith(d)

    # search by: exact description match, prefix description
    #  match, exact hash match, prefix hash match
    for match_fn in (exact_descr_match, prefix_descr_match,
                     exact_match, prefix_match):
        matches = [e for e in exps if match_fn(e, s)]
        if len(matches) > 0:
            return matches



# should have some kind of index for this
def find(descr, exps=None):
    if exps is None:
        exps = read_descrs()

    matches = match(descr, exps)

    # note: descending sort
    matches.sort(lambda x, y: cmp(y['date'], x['date']))

    return matches

def expand_command(cmd, params):
    """Replace special sequences in cmd with appropriate paths specifying
    output directory and input from other experiments

    Note that the experimental hash itself is not inserted here, since it
    must be computed from the output of this function"""

    exps = read_descrs()

    deps = []

    if params is not None:
        used_params = dict.fromkeys(params.keys(), False)

    def expander(m):
        if m.group(1) == '':
            # let this be handled in the next pass
            return '{}'
        elif m.group(1)[0] == ':':
            # parameter substition
            used_params[m.group(1)[1:]] = True
            return str(params[m.group(1)[1:]])
        else:
            exp_hshs = find(m.group(1), exps=exps)

            if len(exp_hshs) == 0:
                print 'Error: could not match %s' % m.group(1)
                exit(1)

            if len(exp_hshs) > 1:
                print 'Warning: found multiple matches for %s' % m.group(1)
                print 'Using latest (%s)' % time.ctime(exps[exp_hshs[0]]['date'])

            deps.append(exp_hshs[0])
            return os.path.join(abs_root_path(), RESULTS_PATH, exp_hshs[0])

    expanded_cmd = (re.sub('{(.*?)}', expander, cmd), deps)

    if params is not None and not all(used_params.values()):
        print 'Warning: not all parameters were used'

    return expanded_cmd

def check_setup():
    """Check that this repository is setup for running experiments, and
    set it up if it isn't"""

    rootdir = abs_root_path()

    expdir = os.path.join(rootdir, EXP_PATH)
    resultsdir = os.path.join(rootdir,  RESULTS_PATH)

    for path, name in [(expdir, 'Experimental'), (resultsdir, 'Results')]:
        if not os.path.isdir(path):
            print name + ' directory does not exist, creating it...'
            try:
                os.mkdir(path)
            except:
                print 'Could not create directory, aborting'
                exit(1)

def parse_params(params_str):
    """Parse a parameter in the string in the from 'k1:v1 k2:v2 ...' into
    a dictionary"""

    if args.params is None:
        return None
    
    params = {}
    for param in params_str.split():
        k, v = param.split(':')
        params[k] = float(v)

    return params

def run_exp(args):
    # make experiment directories if necessary
    check_setup()

    # find out the hash of experimental commit
    hsh = exec_output(['git', 'rev-parse', 'HEAD']).strip()
    
    # command expansion must be done in two phases:
    #  first, inputs are expanded and indentified
    #  then, the experimental hash is computed and substituted

    # parse parameters from command line
    params = parse_params(args.params)

    # lookup and insert necessary inputs
    new_cmd, deps = expand_command(args.command, params)

    rootdir = abs_root_path()
    resultsdir = os.path.join(rootdir, RESULTS_PATH)
    working_dir = os.path.relpath(os.getcwd(), rootdir)

    # compute a hash unique to this experiment
    # this encoding is not totally fool-proof; maybe look to git
    #  for ideas?
    exp_hsh = sha1(hsh + str(len(working_dir)) +
                   working_dir + str(len(args.command)) + new_cmd)

    exp_path = os.path.join(resultsdir, exp_hsh)

    # save to a log file
    new_cmd = new_cmd + ' | tee {}/log 2>&1'

    # substitute in cmd where necessary
    new_cmd = new_cmd.replace('{}', exp_path)


    if not os.path.isdir(resultsdir):
        os.mkdir(resultsdir)

    # check if this experiment has been run
    exp_path = os.path.join(resultsdir, exp_hsh)
    if handle_existing(exp_hsh):
        if args.rerun:
            print 'Rerunning...'
            shutil.rmtree(exp_path)
        else:
            exit(1)

    # the experimental description
    info = {'commit': hsh, 'command': args.command, 'date': time.time(),
            'description': args.description,
            'working_dir': working_dir, 'deps': deps}

    if params is not None:
        info['params'] = params

    # import existing results if requested
    # awkward name conflict here
    if vars(args)['import']:
        matches = find(args.description)

        if len(matches) == 0:
            print 'Could not find matching experiment to import from'
            exit(1)

        print 'Importing results from previous experiment %s' % trunc(matches[0], 6)

        info['import'] = matches[0]
        import_dir = os.path.join(abs_root_path(), RESULTS_PATH, matches[0])

        shutil.copytree(import_dir, exp_path)
    else:
        # create the results directory for this experiment
        # (should only do this once we know we can at least write info file)
        os.mkdir(exp_path)

    # save the experimental description
    save_descr(os.path.join(exp_path, DESCR_FILE), info)
    
    expdir = os.path.join(rootdir, EXP_PATH, exp_hsh)

    # make a directory for this experiment
    try:
        try:
            os.mkdir(expdir)
        except OSError:
            print 'Experimental directory could not be created or already exists.'
            print 'Aborting.'
            exit(1)

        if args.subdir_only:
            checkout_dir = working_dir
        else:
            checkout_dir = '.'

        # checkout the appropriate commit
        # can do this with git --work-tree=... checkout commit -- ., but
        # cannot do concurrently, so use git archive...

        # ... whose behavior seems to depend on current directory
        os.chdir(rootdir)
        sts = exec_shell('git archive {} {} | tar xC {}'
                         .format(args.commit, checkout_dir, expdir))
        if sts != 0:
            print 'Attempt to checkout experimental code failed'
            exit(1)
            
        # go to the experimental directory
        os.chdir(os.path.join(expdir, working_dir))

        # run the experiment
        print 'Running command ' + new_cmd
        sts = exec_shell(new_cmd)
        print 'Command exited with status ' + str(sts)
        info['exit_status'] = sts
        info['date_end'] = time.time()
        save_descr(os.path.join(exp_path, DESCR_FILE), info)
    finally: 
        # clear the experimental directory
        shutil.rmtree(expdir)

def recursive_has_key(d, k, deps):
    if k not in d:
        return False

    for dep in d[k][deps]:
        if not recursive_has_key(d, dep, deps):
            return False

    return True

# should probably store some kind of index to avoid linear search
def read_descrs(keep_unreadable=False, keep_unfinished=False, keep_failed=False):
    resultsdir = os.path.join(abs_root_path(), RESULTS_PATH)
    exp_dirs = os.listdir(resultsdir)

    exps = []

    for exp_dir in exp_dirs:
        exp = Exp(exp_dir)
        #try:
        #    exp = Exp(exp_dir)
        #except Exception as e:
        #    if keep_unreadable:
        #        exps[exp_dir] = None

        if (exp.success() or
            (exp.failure() and keep_failed) or
            keep_unfinished):
            if not exp.broken_deps():
                exps.append(exp)

    return exps

def dominates(exp0, exp1):
    return compare(exp0, exp1) > 0

# this function doesn't return what you think it does
def compare(exp0, exp1):
    """Defines when the results of one experiment should supercede the
    results of another."""

    if exp0 == exp1:
        return 0

    # note that this may not always be what we want; sometimes we want
    #  the latest code instead of the latest run

    if (exp0['description'] != exp1['description'] or
        exp0.get('params') != exp1.get('params')):
        return None 

    if exp0['deps'] == exp1['deps']:
        if exp0['date'] > exp1['date']:
            return 1
    
    if len(exp0['deps']) != len(exp1['deps']):
        return None
    
    # note that there is a dependency ordering problem here
    #  and another subtle bug
    if (all(compare(*x) >= 0 for x in zip(exp0.deps(), exp1.deps())) and
        any(dominates(*x) for x in zip(exp0.deps(), exp1.deps()))):
        return 1

def find_latest(exp_id):
    # remove dominated experiments

    matches = find(exp_id)
    
    # this is not very efficient
    i = 0
    while i < len(matches):
        if any(dominates(x, matches[i]) for x in matches):
            del matches[i]
        else:
            i += 1

    return matches
        

# modifies exps in place!
def list_descrs(exps):

    # sort and group experiments
    exps.sort(lambda x, y: cmp(y['date'], x['date']))
    
    exp_groups = []
    while exps:
        matches = [e for e in exps
                   if e['description'] == exps[0]['description']]

        exp_groups.append(matches)
        for exp in matches:
            exps.remove(exp)
            
    for exp_group in exp_groups:
        print '{:32}  {:4} experiments  last: {}'.format(
                trunc(exp_group[0]['description'], 30),
                len(exp_group),
                time.ctime(exp_group[0]['date']))
        print '  last command:', exp_group[0]['command']
        commits = {e['commit'] for e in exp_group}
        
        code = ' '.join([trunc(hsh, 6) for hsh in commits])

        has_params = filter(lambda e: 'params' in e and e['params'] is not None,
                            exp_group)
        params = sum([e['params'].keys() for e in has_params], [])

        params_dict = dict((k, [e['params'][k] for e in exp_group]) for k in params)

        print '  code: {}  params: {}'.format(code, params_dict)

def purge(args):
    matches = find(exps, args.exp)
    
    if len(matches) > 1 and not args.all:
        print 'Multiple matching experiments; use --all to purge them all'
        return

    resultsdir = os.path.join(abs_root_path(), RESULTS_PATH)
    for hsh in matches:
        print 'Purging {} ({})'.format(exps[hsh]['description'], hsh)
        if not args.dry_run:
            try:
                shutil.rmtree(os.path.join(resultsdir, hsh))
            except Exception as e:
                print 'Could not remove directory: ', e

def print_hashes(args):
    if args.latest:
        matches = find_latest(args.exp)
    else:
        matches = find(args.exp)

    for match in matches:
        print match.hsh


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Track content created by code')
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser('run', help='run an experiment')
    run_parser.add_argument('--import', action='store_true', help='import results from last run of this experiment')
    run_parser.add_argument('--params', help='experimental parameter list')
    run_parser.add_argument('--subdir-only', action='store_true', help='only checkout the contents of current directory')
    run_parser.add_argument('--rerun', action='store_true', help='rerun this experiment, deleting existing results if necessary')
    run_parser.add_argument('commit', help='git commit expression indicating code to run')
    run_parser.add_argument('command', help='command to run')
    run_parser.add_argument('description', help='unique description of this experiment')
    run_parser.set_defaults(func=run_exp)

    list_parser = subparsers.add_parser('list', help='list previous experiments')
    list_parser.set_defaults(func=lambda args: list_descrs(read_descrs(keep_unreadable=True)))
    
    purge_parser = subparsers.add_parser('purge', help='delete experimental data')
    purge_parser.add_argument('--dry-run', action='store_true')
    purge_parser.add_argument('--all', action='store_true', help='purge all experiments matching arguments')
    purge_parser.add_argument('exp', help='experiment identifier')
    purge_parser.set_defaults(func=purge)
    
    hash_parser = subparsers.add_parser('hash', help='print experimental hashes')
    hash_parser.add_argument('--latest', action='store_true', help='include only non-dominated experiments')
    hash_parser.add_argument('exp', help='experiment identifier')
    hash_parser.set_defaults(func=print_hashes)

    args = parser.parse_args()
    args.func(args)
