import subprocess
import os
import time
import shutil
import exp

# TODO: distinguish different failure modes
[RUN_STATE_VIRGIN, RUN_STATE_RUNNING, RUN_STATE_SUCCESS, RUN_STATE_FAIL] = range(4) 

RESULTS_PATH = 'results'
EXP_PATH = 'exp'
DESCR_FILE = 'descr'

def load_info(hsh):
    """Load info about an experiment as saved by save_descr"""
    print "reading from", os.path.join(exp.abs_root_path(), RESULTS_PATH, hsh, DESCR_FILE)

    try:

        f = open(os.path.join(exp.abs_root_path(), RESULTS_PATH, hsh, DESCR_FILE))

    except IOError as e:
        print "new file"
        return None
    else:

        return eval(f.read())




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
                exp.save_descr(os.path.join(node.exp_results, DESCR_FILE), node.info)
                if node.info['run_state'] == RUN_STATE_FAIL:
                    return RUN_STATE_FAIL
        return RUN_STATE_SUCCESS

class dag_node:
     
    def __init__(self, desc, params, commit, command = None, code = None, parents = None, children = None):


	# Creating the new command
        self.new_cmd, deps = exp.expand_command(command, params)

	#  A bunch of directories we will need later on
        rootdir = exp.abs_root_path()
        self.working_dir = os.path.relpath(os.getcwd(), rootdir)
        self.hsh = exp.sha1(commit + str(len(self.working_dir)) +
                   self.working_dir + str(len(command)) + self.new_cmd)

	self.resultsdir = os.path.join(rootdir, RESULTS_PATH)
      	self.exp_results = os.path.join(self.resultsdir, self.hsh)
        self.expdir = os.path.join(rootdir, EXP_PATH, self.hsh)

	self.new_cmd = self.new_cmd + ' | tee {}/log 2>&1'
	self.new_cmd = self.new_cmd.replace('{}', self.exp_results)

        # DAG structure pointers
        self.parents = set()
        self.children = set()
        if parents is not None:
            self.parents.union(parents)
        if children is not None:
            self.children.union(children)
        self.visited = False

        # try to read run info from disk
        self.info = load_info(self.hsh)

        # if not found, intialize from scratch
        if self.info is None:
            self.info = dict()

            self.info['description'] = desc # description (string)
            self.info['working_dir'] = self.working_dir

            # TODO: figure out how to handle these implicit dependencies
            # The dependencies will be filled in later once the parents are finished. See setup_env.
            self.info['deps'] = set([x.hsh for x in self.parents] + deps)


            # exactly one of these should be set
            if command is not None and code is not None:
                raise Exception("command and code are mutually exclusive")


            self.info['command'] = command # command to run (string)
            self.info['code'] = code # code to execute

            self.info['commit'] = commit # commit hash (string)
            self.info['data'] = time.time()
            self.info['params'] = params # parameters to pass (dictionary)

            self.info['run_state'] = RUN_STATE_VIRGIN
            self.info['return_code'] = None
      

        self.jobid = None

    def add_parents(self, parents):
        self.parents.union(parents)
        for parent in parents:
            parent.children.union(self)
            self.info['deps'] += set([x.hsh for x in parents])

    def add_children(self, children):
        self.children.union(children)
        for child in children:
            child.parents.union(self)
            child.info['deps'] += set([self.hsh,])

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
	
	# Make the results directory for this experiment
        if not os.path.isdir(self.exp_results):
            os.mkdir(self.exp_results)
	
	# Save the description and info
	exp.save_descr(os.path.join(self.exp_results, DESCR_FILE), self.info);

	# Make the experiment directories and checkout code. Do it here so that 
	# you fail in the root node of the cluster, if you fail
        if os.path.isdir(self.expdir):
            shutil.rmtree(self.expdir)
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
        rootdir=exp.abs_root_path()
        os.chdir(rootdir)
        sts = exp.exec_shell('git archive {} {} | tar xC {}'
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
        shutil.rmtree(self.expdir)          

