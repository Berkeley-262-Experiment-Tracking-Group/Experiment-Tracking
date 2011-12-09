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
        self.parents.add(parents)
        self.children.add(children)
        self.visited = False
        

        new_cmd, deps = expand_command(command, params)

        hsh = exec_output(['git', 'rev-parse', commit]).strip()
        rootdir = abs_root_path()
        self.working_dir = os.path.relpath(os.getcwd(), rootdir)
        self.hash = sha1(hsh + str(len(working_dir)) +
                   working_dir + str(len(command)) + new_cmd)

        self.run_state = self.readStateFromDisk() 

        self.jobid = None
        self.exp_dir = None


    def add_parents(self, parents):
        self.parents.add(parents)
        for parent in parents:
            parent.children.add(self)

    def add_children(self, children):
        self.children.add(children)
        for child in children:
            child.parents.add(self)

    def read_state_from_disk(self):
        with open(os.path.join(abs_root_path(), RESULTS_PATH, self.hsh, DESCR_FILE)) as f:
            info = eval(f.read())
            
            # TODO: figure out exactly what state it makes sense to store on / load from disk

    def is_runnable(self):
        parents_succeeded = all([p.run_state == RUN_STATE_SUCCESS for p in self.parents])
        return run_state == RUN_STATE_VIRGIN and parents_succeded
            
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



    
