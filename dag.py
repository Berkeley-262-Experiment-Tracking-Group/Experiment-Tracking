import subprocess
import os
import time
import shutil
import exp
import hashlib
import re

# TODO: distinguish different failure modes
[RUN_STATE_VIRGIN, RUN_STATE_RUNNING, RUN_STATE_SUCCESS, RUN_STATE_FAIL] = range(4) 

RESULTS_PATH = 'results'
EXP_PATH = 'exp'
DESCR_FILE = 'descr'

def load_info(hsh):
    """Load info about an experiment as saved by save_descr"""
    print "reading from", os.path.join(abs_root_path(), RESULTS_PATH, hsh, DESCR_FILE)

    try:

        f = open(os.path.join(abs_root_path(), RESULTS_PATH, hsh, DESCR_FILE))

    except IOError as e:
        print "new file"
        return None
    else:

        return eval(f.read())

# Stuff imported from exp. Decided to put it here since closely linked with dag_nodes

# Shortcuts for running shell commands

def exec_cmd(args):
    p = subprocess.Popen(args)
    return os.waitpid(p.pid, 0)[1]

def exec_shell(cmd):
    p = subprocess.Popen(cmd, shell=True)
    return os.waitpid(p.pid, 0)[1]


def exec_output(args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

def abs_root_path():
    return exec_output(['git', 'rev-parse', '--show-toplevel']).strip()

def save_descr(path, info):
    """Save info about an experiment to a file

    info is intended to be a dictionary of objects for which repr does the
    right thing"""

    with open(path, 'w') as f:
        f.write(repr(info))
        f.write('\n')

def sha1(s):
    return hashlib.sha1(s).hexdigest()

# Helper Functions for filling in commands.

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
def find(descr, nodes):
    #if exps is None:
    #    exps = read_descrs()

    matches = match(descr, nodes)

    # note: descending sort
    matches.sort(lambda x, y: cmp(y.info['date'], x.info['date']))

    return matches


# Copied from exp with minor changes. Might have to change drastically based on Allie's description
# Right now, it seems, has 4 cases. Output is written as {}. Parameters are written as {:c}, dependencies without parameters are written as
# {parent} and dependency with params are written as {parent:c}. Will probably have to wait till Allie's input.
def expand_command(cmd, params, parent_nodes):
    """Replace special sequences in cmd with appropriate paths specifying
    output directory and input from other experiments

    Note that the experimental hash itself is not inserted here, since it
    must be computed from the output of this function"""

    # Should not need to read any experiments from disk, Instead just search among parents.
    # exps = read_descrs()

    deps = []

    if params is not None:
        used_params = dict.fromkeys(params.keys(), False)

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
                print d
            else:
                d = m.group(1)

            matched_exps = find(d, parent_nodes)

	    # If something is not matched, this just gives up. Should we allow the user to create dependencies on the fly?
            if len(matched_exps) == 0:
                print 'Error: could not match %s. Did you specify it as a dependency?' % m.group(1)
                exit(1)

            if len(matched_exps) > 1:
                print 'Warning: found multiple matches for %s' % m.group(1)
                print 'Using latest (%s)' % time.ctime(matched_exps[0]['date'])

            deps.append(matched_exps[0].hsh)
            return os.path.join(abs_root_path(), RESULTS_PATH, matched_exps[0].hsh)

    expanded_cmd = (re.sub('{(.*?)}', expander, cmd), deps)

    if params is not None and not all(used_params.values()):
        print 'Warning: not all parameters were used'

    return expanded_cmd









class dag:

    def __init__(self, toplevel_nodes, backend=None):

        self.backend = None
        
        # sort nodes topologically into dag_nodes
        self.dag_nodes_reversed = []
        for n in toplevel_nodes:
            self.visit(n)
        self.dag_nodes = list(reversed(self.dag_nodes_reversed))
	for n in self.dag_nodes:
            n.visited = False
            n.job_init()

    # helper method for topological sort
    def visit(self, node):
        if node.visited == False:
            node.visited=True
            for m in node.children:
                self.visit(m)
            self.dag_nodes_reversed.append(node)

    def update_states(self):
        for node in self.dag_nodes:
            if node.info['run_state'] == RUN_STATE_RUNNING:
                node.info['run_state'], node.info['return_code'] = self.backend.get_state(node)
                if node.info['run_state'] != RUN_STATE_RUNNING:
                    
                    print "cleaning up"
                    node.clean_up_run()
                
    def run_runnable_jobs(self):
        for node in self.dag_nodes:
            if node.is_runnable():
                node.run(self.backend)

    def finished_running(self):
        for node in self.dag_nodes:
            if node.info['run_state'] == RUN_STATE_VIRGIN or node.info['run_state'] == RUN_STATE_RUNNING:
                return RUN_STATE_RUNNING
            
            else:
                save_descr(os.path.join(node.exp_results, DESCR_FILE), node.info)
                if node.info['run_state'] == RUN_STATE_FAIL:
                    return RUN_STATE_FAIL
        return RUN_STATE_SUCCESS

class dag_node:
     
    def __init__(self, desc, params, commit, command = None, code = None, parents = None, children = None):
        # DAG structure pointers
        self.parents = set()
        self.children = set()
        if parents is not None:
            self.parents=self.parents.union(parents)
        if children is not None:
            self.children=self.children.union(children)
        self.visited = False
        # exactly one of these should be set
        if command is not None and code is not None:
            raise Exception("command and code are mutually exclusive")
	self.command=command
	self.code=code
	self.commit=commit
	self.params=params
	self.desc=desc        


    # Have to initialize the hash and all separately, after the parents have been filled. This is because the hash 
    # should use the new command after filling in the hashes of parents and the parameters, and so must be done in
    # topological order.
    def job_init(self):
    	
    	
        # Creating the new command
        self.new_cmd, deps = expand_command(self.command, self.params, self.parents)

	#  A bunch of directories we will need later on
        rootdir = abs_root_path()
        self.rootdir=rootdir
        self.working_dir = os.path.relpath(os.getcwd(), rootdir)
        self.hsh = sha1(self.commit + str(len(self.working_dir)) +
                   self.working_dir + str(len(self.command)) + self.new_cmd)

	self.resultsdir = os.path.join(rootdir, RESULTS_PATH)
      	self.exp_results = os.path.join(self.resultsdir, self.hsh)
        self.expdir = os.path.join(rootdir, EXP_PATH, self.hsh)

	self.new_cmd = self.new_cmd + ' | tee {}/log 2>&1'
	self.new_cmd = self.new_cmd.replace('{}', self.exp_results)
	# try to read run info from disk
        self.info = load_info(self.hsh)

        # if not found, intialize from scratch
        if self.info is None:
            self.info = dict()

            self.info['description'] = self.desc # description (string)
            self.info['working_dir'] = self.working_dir

            # TODO: figure out how to handle these implicit dependencies
            # The dependencies will be filled in later once the parents are finished. See setup_env.
            self.info['deps'] = set([x.hsh for x in self.parents] + deps)


            

            self.info['command'] = self.command # command to run (string)
            self.info['code'] = self.code # code to execute

            self.info['commit'] = self.commit # commit hash (string)
            self.info['data'] = time.time()
            self.info['params'] = self.params # parameters to pass (dictionary)

            self.info['run_state'] = RUN_STATE_VIRGIN
            self.info['return_code'] = None
      

        self.jobid = None


    def add_parents(self, parents):
        self.parents=self.parents.union(parents)
        for parent in list(parents):
            parent.children.add(self)
            
           
            #self.info['deps'] += set([x.hsh for x in parents])

    def add_children(self, children):
        self.children=self.children.union(children)
        for child in list(children):
            child.parents.add(self)
            #child.info['deps'] += set([self.hsh,])

    def is_runnable(self):
        parents_succeeded = all([p.info['run_state'] == RUN_STATE_SUCCESS for p in self.parents])
        return self.info['run_state'] == RUN_STATE_VIRGIN and parents_succeeded

    def fill_in_dependencies(self):
	#Empty function. Fill in self.new_cmd based on the hashes of parents. Basically replace "expname" by "resultsdir/hash" or sth
        pass


    def setup_env(self):
        # Most of this copied from exp.py. exp.py has loads of other stuff that might require figuring out
	# Since the experiment has not been run before, assuming the corresponding directories
	# don't exist
        
	# fill in dependencies in newcmd. Big TODO
	# self.new_cmd=self.fill_in_dependencies();

	# Create results directory if it doesn't exist
	if not os.path.isdir(self.resultsdir):
            os.mkdir(self.resultsdir)

	
	# Create experiments directory if it doesn't exist
	if not os.path.isdir(os.path.join(self.rootdir, EXP_PATH)):
	    os.mkdir(os.path.join(self.rootdir, EXP_PATH))
	    
	# Make the results directory for this experiment
        if not os.path.isdir(self.exp_results):
            os.mkdir(self.exp_results)
	
	# Save the description and info
	save_descr(os.path.join(self.exp_results, DESCR_FILE), self.info);

	# Make the experiment directories and checkout code. Do it here so that 
	# you fail in the root node of the cluster, if you fail
        if os.path.isdir(self.expdir):
            shutil.rmtree(self.expdir)
	print(self.expdir)
	try:
            os.mkdir(self.expdir)
        except OSError:
            print 'Experimental directory could not be created or already exists.'
            print 'Aborting.'
            exit(1)

	# Some args which we may or may not have access to
        #if args.subdir_only:
        #    checkout_dir = working_dir
        #else:
        checkout_dir = '.'
        
        # checkout the appropriate commit
        # can do this with git --work-tree=... checkout commit -- ., but
        # cannot do concurrently, so use git archive...

        # ... whose behavior seems to depend on current directory
        rootdir=abs_root_path()
        os.chdir(rootdir)
        sts = exec_shell('git archive {} {} | tar xC {}'
                         .format(self.info['commit'], checkout_dir, self.expdir))
        if sts != 0:
            print 'Attempt to checkout experimental code failed'
            exit(1)

    def run(self, black_box):

        print "run - setup_env"

        # TODO: implement setup_env (based on exp.run_exp())
        self.setup_env()

        print "run - executing command"
        if self.info['code'] is not None:
            try:
                self.info['return_code'] = eval(self.info['code'])
                self.info['run_state'] = RUN_STATE_SUCCESS
            except:
                self.info['run_state'] = RUN_STATE_FAIL
        else:
            self.jobid = black_box.run(self)
            self.info['run_state'] = RUN_STATE_RUNNING

    def clean_up_run(self):
        # Need to cd back out of expdir
    	os.chdir(os.path.join(self.rootdir, self.working_dir))
        shutil.rmtree(self.expdir)          

