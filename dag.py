import exp


# TODO: distinguish different failure modes
[RUN_STATE_VIRGIN, RUN_STATE_RUNNING, RUN_STATE_SUCCESS, RUN_STATE_FAIL] = range(4) 

def abs_root_path():
    return exec_output(['git', 'rev-parse', '--show-toplevel']).strip()

class dag:

    def __init__(self, toplevel_nodes):

        # sort nodes topologically into dag_nodes
        self.dag_nodes_reversed = []
        for n in toplevel_nodes:
            self.visit(n)
        self.dag_nodes = [x for x in reversed(self.dag_nodes_reversed)]
        for n in dag_nodes:
            n.visited = False

    # helper method for topological sort
    def visit(self, node):
        if node.visited == False:
            node.visited=True
            for m in node.children:
                self.visit(m)
            self.dag_nodes_reversed.append(node)

    def update_states(self, black_box):
        for node in self.dag_nodes:
            if node.run_state == NODE_STATE_RUNNING:
                node.run_state = black_box.getState(node)
                if node.run_state != NODE_STATE_RUNNING:
                    node.clean_up_run()
                

    def run_runnable_jobs(self, black_box):
        for node in self.dag_nodes:
            if node.is_runnable():
                node.run(black_box)

class dag_node:
     
    def __init__(self, desc, params, commit, command = None, code = None, parents = None, children = None):
        
        self.desc = desc # description (string)

        # exactly one of these should be set
        if command is not None and code is not None:
            raise Exception("command and code are mutually exclusive")


       #self.info = {'commit': hsh, 'command': command, 'date': time.time(),
       #         'description': desc,
       #         'working_dir': working_dir, 'deps': deps}

        self.command = command # command to run (string)
        self.code = code # code to execute

        self.params = params # parameters to pass (dictionary)
        self.commit = commit # commit hash (string)


        self.parents = set()
        self.children = set()
        if parents is not None:
            self.parents.union(parents)
        if children is not None:
            self.children.union(children)
        self.visited = False
        

        self.new_cmd, deps = expand_command(command, params)
	
	#  A bunch of directories we will need later on
        hsh = exec_output(['git', 'rev-parse', commit]).strip()
        rootdir = abs_root_path()
        self.working_dir = os.path.relpath(os.getcwd(), rootdir)
        self.hsh = sha1(hsh + str(len(working_dir)) +
                   working_dir + str(len(command)) + new_cmd)
	self.resultsdir = os.path.join(rootdir, RESULTS_PATH)
	
	self.exp_path = os.path.join(self.resultsdir, exp_hsh)
	self.expdir = os.path.join(rootdir, EXP_PATH, exp_hsh)

	# Creating the new command
	self.new_cmd = self.new_cmd + ' | tee {}/log 2>&1'
	self.new_cmd = self.new_cmd.replace('{}', exp_path)
        #The dependencies will be filled in later once the parents are finished. See setup_env.

        self.run_state = self.readStateFromDisk() 

        self.jobid = None
        self.exp_dir = None


    def add_parents(self, parents):
        self.parents.union(parents)
        for parent in parents:
            parent.children.union(self)

    def add_children(self, children):
        self.children.union(children)
        for child in children:
            child.parents.union(self)

    def read_state_from_disk(self):
        # TODO: figure out exactly what state it makes sense to store on / load from disk
	# If the experiment directory exists, then currently assumes experiment is successful
        # TODO: Make sure this code calls the right parameters, i.e handle_existing uses hsh
        # Currently uses exit status inside the descr file. Better ways to do that?
        if handle_existing(self.hsh):
	    info=load_info(self.hsh)
	    if(info['exit_status']==0):
	        return RUN_STATE_SUCCESS
            else
                return RUN_STATE_FAILURE   
	else
	   return RUN_STATE_VIRGIN


        #with open(os.path.join(abs_root_path(), RESULTS_PATH, self.hsh, DESCR_FILE)) as f:
        #    info = eval(f.read())
            

    def is_runnable(self):
        parents_succeeded = all([p.run_state == RUN_STATE_SUCCESS for p in self.parents])
        return self.run_state == RUN_STATE_VIRGIN and parents_succeded

    
    def fill_in_dependencies(self):
	#Empty function. Fill in self.new_cmd based on the hashes of parents. Basically replace "expname" by "resultsdir/hash" or sth


    def setup_env(self):
        # Most of this copied from exp.py. exp.py has loads of other stuff that might require figuring out
	# Since the experiment has not been run before, assuming the corresponding directories
	# don't exist
        
	#fill in dependencies in newcmd. Big TODO
	self.new_cmd=self.fill_in_dependencies();

	# Create results directory if it doesn't exist
	if not os.path.isdir(self.resultsdir):
            os.mkdir(self.resultsdir)
	
	#Info to be written
	info = {'commit': self.hsh, 'command': self.command, 'date': time.time(),
            'description': self.descr,
            'working_dir': self.working_dir, 'deps': [x.hsh for x in self.parents]}
	
	# Make the results directory for this experiment
	os.mkdir(self.exp_path)
	
	# Save the description and info
	save_descr(os.path.join(self.exp_path, DESCR_FILE), info);

	# Make the experiment directories and checkout code. Do it here so that 
	# you fail in the root node of the cluster, if you fail
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
                         .format(self.commit, checkout_dir, self.expdir))
        if sts != 0:
            print 'Attempt to checkout experimental code failed'
            exit(1)


	

        
    def run(self, black_box):

        # TODO: implement setup_env (based on exp.run_exp())
        self.setup_env()

        if node.code is not None:
            try:
                eval(node.code)
                node.run_state = RUN_STATE_SUCCESS
            except:
                node.run_state = RUN_STATE_FAIL
        else:
            self.jobid = black_box.run(node)
            node.run_state = RUN_STATE_RUNNING

    def clean_up_run(self):
        shutil.rmtree(expdir)          

#while not done running everything:
#    poll and update run state at each node
#    walk over nodes, and run each node if possible



    
