import os
import time
import shutil
import re

import util, exp_common
import special_macros
# TODO: distinguish different failure modes
[RUN_STATE_VIRGIN, RUN_STATE_RUNNING, RUN_STATE_SUCCESS, RUN_STATE_FAIL] = range(4) 

# TODO: make this a command-line option
MAX_PROCESSES = 2


def save_descr(path, info):
    """Save info about an experiment to a file

    info is intended to be a dictionary of objects for which repr does the
    right thing"""

    with open(path, 'w') as f:
        f.write(repr(info))
        f.write('\n')

def load_info(hsh):
    """Load info about an experiment as saved by save_descr"""
    try:
        f = open(os.path.join(util.abs_root_path(), 
                              exp_common.RESULTS_DIR, hsh, exp_common.DESCR_FILE))
    except IOError as e:
        return None
    else:
        return eval(f.read())

# Helper Functions for filling in commands.

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
            
            self.propagate_params(n)
            n.job_init()
            if n['run_state'] == RUN_STATE_SUCCESS:
                print "Job '%s' has already completed successfully, skipping..." % (n['description'])


    # helper method for topological sort
    def visit(self, node):
        if node.visited == False:
            node.visited=True
            for m in node.children:
                self.visit(m)
            self.dag_nodes_reversed.append(node)

    def mainloop(self):
         while self.finished_running() == RUN_STATE_RUNNING:
             self.run_runnable_jobs()
             time.sleep(1)
             self.update_states()
         return self.finished_running()

    def update_states(self):
        for node in self.dag_nodes:
            if node.info['run_state'] == RUN_STATE_RUNNING:
                node.info['run_state'], node.info['return_code'] = self.backend.get_state(node)
                if node.info['run_state'] == RUN_STATE_SUCCESS:
                    node.clean_up_run()
                
    def run_runnable_jobs(self):
        running = 0
        for node in self.dag_nodes:
            if node.info["run_state"] == RUN_STATE_RUNNING:
                running += 1
            if node.is_runnable() and running < MAX_PROCESSES:
                node.run(self.backend)
                running += 1

    def finished_running(self):
        for node in self.dag_nodes:
            if node.info['run_state'] == RUN_STATE_VIRGIN or node.info['run_state'] == RUN_STATE_RUNNING:
                return RUN_STATE_RUNNING
            
            else:
                save_descr(os.path.join(node.exp_results, exp_common.DESCR_FILE), node.info)
                if node.info['run_state'] == RUN_STATE_FAIL:
                    return RUN_STATE_FAIL
        return RUN_STATE_SUCCESS
    
    # Propagate parameters along dag. Thus each experiment has a history of the parameters of its ancestors 
    # Important note: the propagated parameters have values  that are lists, to allow for multiple parents with the same descr 
    # There must be a more 'pythonic' way

    def propagate_params(self, node):
        for p in node.parents:
            for param in p.params:
                new_param=p.desc+':'+param
                if(new_param in node.params):

                    # The first time we see a parameter, it's stored
                    # as a singleton. The second time, we make it into
                    # a list.
                    if not isinstance(node.params[new_param], list):
                        node.params[new_param] = [node.params[new_param]]
                    node.params[new_param] += [p.params[param]]
                else:
                    node.params[new_param]=p.params[param]
    
class dag_node:
     
    def __init__(self, desc=None, params={}, commit=None, command = None, code = None, parents = None, children = None, rerun = False, subdir_only = False, hsh = None):

        if hsh is None and (desc is None or commit is None or (command is None and code is None)):
            print "Error: if not specifying hash, must specify description, commit, and either command or code."
            exit(1)
        if hsh is not None and (desc is not None or commit is not None or command is not None or code is not None):
            print "Warning: ignoring description, commit, command, and code since hash is specified."

        # exactly one of these should be set
        if command is not None and code is not None:
            print "Error: command and code are mutually exclusive."
            exit(1)
        self.command=command
        self.code=code
        self.commit=commit
        self.params=params
        self.desc=desc   
        self.hsh = hsh

        # DAG structure pointers
        self.parents = set()
        self.children = set()
        if parents is not None:
            self.parents=self.parents.union(parents)
        if children is not None:
            self.children=self.children.union(children)
        self.visited = False

        self.rerun = rerun
        self.subdir_only = subdir_only

        if hsh is not None:
            self.job_init()

    # Have to initialize the hash and all separately, after the parents have been filled. This is because the hash 
    # should use the new command after filling in the hashes of parents and the parameters, and so must be done in
    # topological order.
    def job_init(self):
    	

        # Creating the new command
        

	#  A bunch of directories we will need later on
        rootdir = util.abs_root_path()
        self.rootdir=rootdir
        self.working_dir = os.path.relpath(os.getcwd(), rootdir)
	self.resultsdir = os.path.join(rootdir, exp_common.RESULTS_DIR)
	
        if self.hsh is None:        
            
            if self.code is None:
                self.new_cmd, deps = exp_common.expand_command(self.command, self.params, self.parents)   
                self.hsh = util.sha1(self.commit + str(len(self.working_dir)) +
                                 self.working_dir + str(len(self.command)) + self.new_cmd)
                self.exp_results = os.path.join(self.resultsdir, self.hsh)
                self.expdir = os.path.join(rootdir, exp_common.EXP_DIR, self.hsh)
                self.new_cmd = self.new_cmd.replace('{}', self.exp_results)
                self.new_code=None
            else:

                # terrible terrible hack to prevent parameter
                # substitution for macros (since this syntax
                # interferes with Python list syntax). TODO: figure
                # out whether this is actually a good idea (hint: no).
                code = self.code.replace("[", "<---")
                code = code.replace("]", "--->")
                new_code, deps = exp_common.expand_command(code, self.params, self.parents)   
                new_code = new_code.replace("<---", "[")
                self.new_code = new_code.replace("--->", "]")
                deps=[x.hsh for x in self.parents]
                self.hsh = util.sha1(self.commit + str(len(self.working_dir)) +
                                 self.working_dir + str(len(self.code)) + self.new_code + repr(deps))
                self.exp_results = os.path.join(self.resultsdir, self.hsh)
                self.expdir = os.path.join(rootdir, exp_common.EXP_DIR, self.hsh)
                self.new_code = self.new_code.replace('{}', self.exp_results)
                self.new_cmd=None
            
            # try to read run info from disk
            self.info = load_info(self.hsh)
            
        else:
            self.info = load_info(self.hsh)
            if self.info is None:
                print "Error: could not load experiment %s." % (self.hsh)
                exit(1)
            self.new_cmd = self.info['final_command']
            self.new_cmd = self.info['final_code']
            self.deps = self.info['deps'] 
            #exp_common.expand_command(self.info["command"], self.info["params"], self.deps())   
            self.desc = self.info['description']
            self.exp_results = os.path.join(self.resultsdir, self.hsh)
            self.expdir = os.path.join(rootdir, exp_common.EXP_DIR, self.hsh)


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
            self.info['date'] = time.time()
            self.info['params'] = self.params # parameters to pass (dictionary)

            self.info['run_state'] = RUN_STATE_VIRGIN
            self.info['return_code'] = None
            
            self.info['final_command']=self.new_cmd
            self.info['final_code']=self.new_code
        else:
            if self.info['description'] != self.desc:
                print "Warning: job description '%s' differs from " \
                    "saved description '%s'; using '%s'" \
                    % (self.desc, self.info['description'], \
                           self.info['description'])
        

            if self.rerun == True:
                self.info['run_state'] = RUN_STATE_VIRGIN
                self.info['return_code'] = None
                self.info['date'] = time.time()
                shutil.rmtree(self.exp_results)


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

    def setup_env(self):

	# Create experiments directory if it doesn't exist
	if not os.path.isdir(os.path.join(self.rootdir, exp_common.EXP_DIR)):
	    os.makedirs(os.path.join(self.rootdir, exp_common.EXP_DIR))
	    
	# Make the results directory for this experiment
        if not os.path.isdir(self.exp_results):
            os.makedirs(self.exp_results)
	
	# Save the description and info
	save_descr(os.path.join(self.exp_results, exp_common.DESCR_FILE), self.info);

	# Make the experiment directories and checkout code. Do it
	# here so that you fail in the root node of the cluster, if
	# you fail
        if os.path.isdir(self.expdir):
            shutil.rmtree(self.expdir)
	try:
            os.mkdir(self.expdir)
        except OSError:
            print 'Experimental directory could not be created or already exists.'
            print 'Aborting.'
            exit(1)


        if self.subdir_only:
            checkout_dir = working_dir
        else:
            checkout_dir = '.'
        
        # checkout the appropriate commit
        # can do this with git --work-tree=... checkout commit -- ., but
        # cannot do concurrently, so use git archive...

        # ... whose behavior seems to depend on current directory
        rootdir=util.abs_root_path()
        os.chdir(rootdir)
        sts = util.exec_shell('git archive {} {} | tar xC {}'
                         .format(self.info['commit'], checkout_dir, self.expdir))
        if sts != 0:
            print 'Attempt to checkout experimental code failed'
            exit(1)

    def run(self, black_box):

        self.setup_env()

        if self.info['code'] is not None:
            try:
                print 'Running code'
                self.info['return_code'] = special_macros.evaluate(self.new_code, self)
                self.info['run_state'] = RUN_STATE_SUCCESS
            except Exception as e:
                print e
                self.info['run_state'] = RUN_STATE_FAIL
        else:
            self.jobid = black_box.run(self)
            self.info['run_state'] = RUN_STATE_RUNNING

    def clean_up_run(self):
        # Need to cd back out of expdir
    	os.chdir(os.path.join(self.rootdir, self.working_dir))
        shutil.rmtree(self.expdir)          


    def __getitem__(self, name):
        return self.info[name]

    def __contains__(self, name):
        return name in self.info

    def get(self, name):
        return self.info.get(name)

    def success(self):
        return self.info['run_state'] == RUN_STATE_SUCCESS

    def failure(self):
        return self.info['run_state'] == RUN_STATE_FAIL

    def running(self):
        """Running or killed... should be able to distinguish these two somehow"""
        return self.info['run_state'] == RUN_STATE_RUNNING

    def deps(self):
        return [dag_node(hsh = hsh) \
                    for hsh in self.info['deps']]

    def find_deps(self, name):
        return exp_common.match(name, self.deps())

    def find_dep(self, name):
        return self.find_deps(name)[0]

    def filename(self, name):
        return os.path.join(abs_root_path(), RESULTS_DIR, self.hsh, name)

    def param(self, name):
        return self.info['params'][name]

    def broken_deps(self, in_dep=False):
        if in_dep and not self.success():
            return True
        try:
            return any(dag_node(hsh = hsh).broken_deps(in_dep=True) for hsh in self.info['deps'])
        except IOError:
            return True
