import os
import time
import re
import util, dag
import sys

DOT_DIR = '.exp'
EXP_DIR = os.path.join(DOT_DIR, 'exp')
RESULTS_DIR = os.path.join(DOT_DIR, 'results')
DESCR_FILE = 'descr'
TASK_DIR = os.path.join(DOT_DIR, 'tasks')
TASK_COMMIT_FILE='commit'

# A hack. Need to do something so that all experiments aren't repeatedly read from disk.
all_nodes=None

# Copied from exp with minor changes. Might have to change drastically based on Allie's description
# Right now, it seems, has 4 cases. Output is written as {}. Parameters are written as {:c}, dependencies without parameters are written as
# {parent} and dependency with params are written as {parent:c}. Will probably have to wait till Allie's input.
def expand_command(cmd, params, parent_nodes = None, have_loaded_all=False):
    """Replace special sequences in cmd with appropriate paths specifying
    output directory and input from other experiments

    Note that the experimental hash itself is not inserted here, since it
    must be computed from the output of this function"""

    # Should not need to read any experiments from disk, Instead just search among parents.

    deps = []

    # In case this is not being run from the DAG, we might need to look up older experiments. This variable stores all experiments,
    # but is not filled in unless actually required.
    
    
    
    
    if params is not None:
        used_params = dict.fromkeys(params.keys(), False)

    # Ugly: Parse parameters with square brackets
    def sqr_expander(m):
         if(m.group(1) not in params):
            print 'Error: Cant find parameter {}'.format(m.group(1))
            exit(1)
         used_params[m.group(1)] = True
         return str(params[m.group(1)])
    cmd_new = re.sub('\[(.*?)\]', sqr_expander, cmd)
    
    
    def expander(m):
        
        if m.group(1) == '':
            # let this be handled in the next pass
            return '{}'
        elif m.group(1)[0] == ':':
            # parameter substition
            # uh oh error checking...
            used_params[m.group(1)[1:]] = True
            return str(params[m.group(1)[1:]])
        else:
            # in-description parameter substitution
            if ':' in m.group(1):
                d, ps = m.group(1).split(':', 1)
                ps = ps.split(',')
                # more param type mess
                d += ':' + ','.join(p + '=' + str(params[p]) for p in ps)

                for p in ps:
                    used_params[p] = True
            else:
                d = m.group(1)

            matched_exps = find(d, parent_nodes)


	    # If something is not matched, this just gives up. Should we allow the user to create dependencies on the fly?
            if len(matched_exps) == 0:
                print 'Warning: could not match %s in the dependency. Did you specify it as a dependency?' % m.group(1)
                var=raw_input('Do you want me to check all older experiments? y/n')
                
                if(var=='y'):
                    global all_nodes
                    if all_nodes is None:
                        all_nodes=read_descrs()
                    #    have_loaded_all = True
                    matched_exps = find(d, all_nodes)
                    if len(matched_exps) == 0:
                        print 'Error: Could not match %s. Aborting.' % m.group(1)
                        exit(1)
                else:
                    print 'Aborting.'        
                    exit(1)

            if len(matched_exps) > 1:
                print 'Warning: found multiple matches for %s' % m.group(1)
                print 'Using latest (%s)' % time.ctime(matched_exps[0]['date'])

            deps.append(matched_exps[0].hsh)
            return os.path.join(util.abs_root_path(), RESULTS_DIR, matched_exps[0].hsh)

    expanded_cmd = (re.sub('{(.*?)}', expander, cmd_new), deps)

    if params is not None and not all(used_params.values()):
        print 'Warning: not all parameters were used'

    return expanded_cmd


# This function matches a description with a node. Copied from exp with minor changes
def match(s, nodes):
    exact_match = lambda x, d: x.hsh == d
    prefix_match = lambda x, d: x.hsh.startswith(d)
    exact_descr_match = lambda x, d: x.info['description'] == d
    prefix_descr_match = lambda x, d: x.info['description'].startswith(d)
    def exact_params_match(x, d):
        if ':' not in d:
            return False
        d, ps = d.split(':', 1)
        ps = [p.split('=', 1) for p in ps.split(',')]
        # should have a single parameter special case
        return (x.info['description'] == d and len(x.info['params']) == len(ps)
                and all(x.info['params'][p[0]] == float(p[1]) for p in ps))

    # search by: exact description match with parameters,
    #  exact description match, prefix description
    #  match, exact hash match, prefix hash match
    for match_fn in (exact_params_match, exact_descr_match, prefix_descr_match,
                     exact_match, prefix_match):
        matches = [e for e in nodes if match_fn(e, s)]
        if len(matches) > 0:
            return matches

    return []

# Find a descr in nodes. Copied from exp with minor changes
def find(descr, nodes=None):
    if nodes is None:
        nodes = read_descrs()

    matches = match(descr, nodes)

    # note: descending sort
    matches.sort(lambda x, y: cmp(y.info['date'], x.info['date']))

    return matches

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
        
def dominates(exp0, exp1):
    return compare(exp0, exp1) > 0

# this function doesn't return what you think it does
def compare(exp0, exp1):
    """Defines when the results of one experiment should supercede the
    results of another."""

    if exp0.hsh == exp1.hsh:
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






# should probably store some kind of index to avoid linear search
def read_descrs(keep_unreadable=False, keep_unfinished=False, keep_failed=False,
                keep_broken_deps=False):
    resultsdir = os.path.join(util.abs_root_path(), RESULTS_DIR)

    try:
        exp_dirs = os.listdir(resultsdir)
        
    except OSError:
        exp_dirs = []

    exps = []

    for exp_dir in exp_dirs:
        exp = dag.dag_node(hsh = exp_dir)
 
        if (exp.success() or
            (exp.failure() and keep_failed) or
            keep_unfinished):
            if keep_broken_deps or not exp.broken_deps():
                exps.append(exp)

    sys.stderr.write('Finished reading descriptions...\n')
    return exps
