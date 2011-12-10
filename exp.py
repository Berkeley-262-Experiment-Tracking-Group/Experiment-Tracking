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
#  - should remove write permissions on results directories
#  - parallelization
#  - job queue
#  - automatic jobs
#  - above should be managed separately?
#  - exp repeat for repeating experiments with new code
#  - exp purge --keep-latest for removing redundant data
#  - what should happen when an experiment fails?
#  - directory sharing for parallel experiments
#  - collapse exp list output a bit
#  - exp hash/exp results
#  - exp show
#  - check on running experiments
#  - don't allow running experiments with broken dependencies
#  - organize this code
#  - check for newer dependencies
#  - use tag names where available
#  - fix params typing mess
#  - exp table for printing tables of a v b where a, b in {code, command, params...}
#  - does find_latest always do the right thing?
#  - fill in previous parameters correctly
#  - exp run --dry-run
#  - fix horrible inefficiencies in exp list &c
#  - matching for exp list
#  - keep track of/fill in --subdir-only option
#  - add a third, more compact display?
#  - exp status for seeing running and completed experiments
#  - piping to 'head' creates a broken pipe error
#  - don't filter out running experiments when filling in previously used arguments
#  - there seems to be a bug in exp run --rerun that doesn't remove directories the first time
#  - keep track of where experiments are running
#  - exp show: remove constant columns, add options for showing/sorting columns
#  - exp show: display available files
#  - exp register
#  - unified messages and verbosity levels
#  - fix plural/singular confusion in messages

import subprocess
import sys
import argparse
import os, os.path
import shutil
import hashlib
import time
import datetime
import re
import dag, util, local_backend

from exp_common import *

RESULTS_PATH = 'results'
EXP_PATH = 'exp'
DESCR_FILE = 'descr'


def parse_params(params_str):
    """Parse a parameter in the string in the from 'k1:v1 k2:v2 ...' into
    a dictionary"""

    if args.params is None:
        return {}
    
    params = {}
    for param in params_str.split():
        k, v = param.split(':')
        params[k] = float(v)

    return params

def fill_last_args(args):
    """Fill in missing args using the ones from the last
    experiment of the same description"""

    # maybe we don't need to pull up the last experiment at all...
    if args.commit and args.command:
        return args

    last_exps = find_latest(args.description)

    if not last_exps:
        return args

    if not args.commit:
        args.commit = last_exps[0]['commit']
        print 'Filling in previous commit', util.trunc(args.commit, 6)

    if not args.command:
        args.command = last_exps[0]['command']
        print 'Filling in previous command', util.trunc(args.command, 30)

    return args

def fill_defaults(args):
    """Fill in default values of arguments. Note that we don't do this
    using argparse because we first try to fill in arguments using
    similar previous experiments"""

    if not args.commit:
        args.commit = 'HEAD'
        print 'Filling in default commit HEAD'

    return args

def check_args(args):
    if not args.command:
        print 'No command specified and no previous matching experiment found.'
        exit(0)

def run_exp(args):
    # command expansion must be done in two phases:
    #  first, inputs are expanded and indentified
    #  then, the experimental hash is computed and substituted

    # if arguments are partially specified, fill them in from previous
    # runs
    args = fill_last_args(args)

    # now fill in any missing arguments that have defaults
    args = fill_defaults(args)

    # now make sure we have all the necessary arguments
    check_args(args)

    # find out the hash of experimental commit
    hsh = util.exec_output(['git', 'rev-parse', args.commit]).strip()

    # parse parameters from command line
    params = parse_params(args.params)

    job = dag.dag_node(args.description, params, hsh, args.command, rerun = args.rerun, subdir_only = args.subdir_only)
    jobs = dag.dag([job,])
    lb = local_backend.local_backend()
    jobs.backend = lb
    jobs.mainloop()


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
                util.trunc(exp_group[0]['description'], 30),
                len(exp_group),
                time.ctime(exp_group[0]['date']))
        print '  last command:', exp_group[0]['command']
        commits = {e['commit'] for e in exp_group}
        
        code = ' '.join([util.trunc(hsh, 6) for hsh in commits])

        has_params = filter(lambda e: 'params' in e and e['params'] is not None,
                            exp_group)
        params = sum([e['params'].keys() for e in has_params], [])

        params_dict = dict((k, [e['params'][k] for e in exp_group]) for k in params)

        print '  code: {}  params: {}'.format(code, params_dict)

def purge(args):
    matches = find(args.exp)
    
    if len(matches) > 1 and not args.all:
        print 'Multiple matching experiments; use --all to purge them all'
        return

    resultsdir = os.path.join(util.abs_root_path(), RESULTS_PATH)
    for exp in matches:
        print 'Purging {} ({})'.format(exp['description'], exp.hsh)
        if not args.dry_run:
            try:
                shutil.rmtree(os.path.join(resultsdir, exp.hsh))
            except Exception as e:
                print 'Could not remove directory: ', e

def print_hashes(args):
    if args.latest:
        matches = find_latest(args.exp)
    else:
        matches = find(args.exp)

    for match in matches:
        print match.hsh

def read_command_args(args):
    orig_cmd = args.command + ' ' + ' '.join(args.args)
    params = parse_params(args.params)

    return expand_command(orig_cmd, params)[0]

def print_command(args):
    print read_command_args(args)
        
def run_command(args):
    cmd = read_command_args(args)
    sys.exit(util.exec_shell(cmd))
    
def show_exp(args):
    # maybe should actually find one particular description,
    #  then get all the experiments of that description

    exp_id = ' '.join(args.exp)

    matches = find(exp_id, read_descrs(keep_unfinished=True, keep_failed=True,
                                       keep_broken_deps=True))

    if len(matches) == 0:
        print 'Could not find matching experiment ' + ' '.join(args.exp)
        exit(1)

    print ('{} experiments found with description "{}"'
            .format(len(matches), exp_id))
    print

    # commands are long, so print out a key
    commands = {exp['command'] for exp in matches}
    command_tab = dict((command, i+1) for i, command in enumerate(commands))

    for command, i in command_tab.iteritems():
        print '{} {}'.format(i, command)
    print

    # find out all possible params
    params = set()
    for exp in matches:
        if 'params' in exp:
            for k in exp['params'].keys():
                params.add(k)

    params = list(params)
    params.sort()

    def param_str(params):
        (params[p] for p in params)

    ndeps = max(len(exp['deps']) for exp in matches)
    if ndeps > 0:
        deps_format = '{{:{}}}'.format(ndeps * 7 - 1)
        deps_header = 'Deps'
    else:
        deps_format = '{}'
        deps_header = ''

    # relying now on chronological sort from find
    format_str = '{:1}{:8} {:25} {:9} {:8} {:3} ' + deps_format + ' {:>8}' * len(params)
    print format_str.format('', 'Hash', 'Start Date', 'Duration', 'Code', 'Cmd', deps_header, *params)
    for exp in matches:
        if exp.running():
            status = '*'
        elif exp.failure():
            status = '!'
        elif exp.broken_deps():
            status = '?'
        else:
            status = ''

        if 'date_end' in exp:
            dur = round(exp['date_end'] - exp['date'])
            duration = datetime.timedelta(seconds=dur)
        elif exp.running():
            dur = round(time.time() - exp['date'])
            duration = str(datetime.timedelta(seconds=dur)) + '+'
        else:
            duration = ''

        print (format_str
                .format(status, exp.hsh[:6],
                    time.ctime(exp['date']),
                    duration,
                    exp['commit'][:6],
                    command_tab[exp['command']],
                    ','.join(dep[:6] for dep in exp['deps']),
                    *(exp['params'][p] for p in params)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Track content created by code')
    subparsers = parser.add_subparsers()

    run_parser = subparsers.add_parser('run', help='run an experiment')

    # Importing is not currently supported using the dag
    # infrastructure. TODO: I don't understand what this is supposed
    # to do.
    # run_parser.add_argument('--import', action='store_true', help='import results from last run of this experiment')

    run_parser.add_argument('--params', help='experimental parameter list')
    run_parser.add_argument('--subdir-only', action='store_true', help='only checkout the contents of current directory')
    run_parser.add_argument('--rerun', action='store_true', help='rerun this experiment, deleting existing results if necessary')
    run_parser.add_argument('description', help='unique description of this experiment')
    run_parser.add_argument('command', nargs='?', help='command to run')
    run_parser.add_argument('commit', nargs='?', help='git commit expression indicating code to run')
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
    
    cmd_parser = subparsers.add_parser('cmd', help='run a command (not an experiment) expanding references')
    cmd_parser.add_argument('--params', help='parameter list')
    cmd_parser.add_argument('command', help='the command')
    cmd_parser.add_argument('args', nargs='*', help='arguments')
    cmd_parser.set_defaults(func=run_command)

    print_parser = subparsers.add_parser('print', help='print a command (not an experiment) expanding references')
    print_parser.add_argument('--params', help='parameter list')
    print_parser.add_argument('command', help='the command')
    print_parser.add_argument('args', nargs='*', help='arguments')
    print_parser.set_defaults(func=print_command)

    show_parser = subparsers.add_parser('show', help='show details of one experiment')
    show_parser.add_argument('exp', nargs='*', help='experiment identifier')
    show_parser.set_defaults(func=show_exp)

    args = parser.parse_args()
    args.func(args)
