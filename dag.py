

class dag_node:
     

    def __init__(self, desc, command, params, commit, parents = None, children = None):
        
        self.desc = desc # description (string)
        self.command = command # command to run (string)
        self.params = params # parameters to pass (dictionary)
        self.commit = commit # commit hash (string)
    
        self.parents += parents
        self.children += children

    def add_parents(self, parents):
        self.parents += parents

    def add_children(self, children):
        self.children += children


        



