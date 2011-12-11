#!/usr/bin/env python

#Parse a file that describes the set of experiments to be run and their dependencies.
#The simplest thing is to assume that everything in the file should be run, but a
#more advanced version of this should take additional arguments besides the file
#and generate a partial dag.

#File has groups of lines for each experiment form:
#command
#\t"description"
#\tCommit (optional)
#\tparam1=?, param2=?, (optional)
#\tparam3=?, param4=? (optional)
#\tDependency1, Dependency2 (optional)

#Dependencies can be one of the following:
#1. "A" - where A is the description of a previously defined experiement. The current experiment depends on A
#2. "A" {all} - again A is the description of another experiement. The current experiment depends on
#               all possible ways of runnning A, for all parameters.
#3. param - The current experiment might have a dependency that is variable and is provided as a parameter.
#           In this case there should be a parameter which is named param which corresponds to a string that
#           matches the description of another experiment.


#TODO: Going to assume that if var_val is a list then there are actually
#seperate different values for that parameter. There is a case where the
#parameter is actually supposed to be a list. This will case problems.

from dag import dag_node, dag
import local_backend
from sets import Set
import argparse
import copy

nodes = {}



def run_file(args):
    f = open(args.file, 'r');
    state = 'start' 
                        
    #nodes is a dictionary mapping descriptions to dag_node objects.
    count = 0;
    for line in f:
        count = count+1
        if line.strip() == '':
            continue
        if state == 'start':
            #This will only be executed once 
            command = line.strip()
            commit = 'HEAD' #default
            state = 'description'
            parameters = {}
            dependencies = Set()
            print("command = " + command)

        elif state == 'description':
            #Reach this point because this line should be the description line
            #Check for tab then "
            if line[0:2] != '\t"':
                print('Error in line ' + str(count) + ' of ' + args.file + ': The first line after each command should start with a tab, followed by the description for the experiment, enclosed in double quotation marks.')
                exit(-1);
            desc = line[2:line.find('"', 2)].strip()
            state = 'commit'
          #  print("desc = " + desc)
            
        #Commit is optional
        elif state == 'commit' and line[0:1] == '\t' and line.find('=') == -1 and line[1] != '{':
            commit = line[1:line.find('\n',1)]
          #  print("commit = " + commit)

        else:
            #Reach this point if the line specifies parameters, dependencies or we have reached a new command
            #Check for tab, if no tab then the experiement description is finished.
            if line[0:1] != '\t':
                #print("Finish " + command)
                check_dependencies(parameters,desc,commit, command,None,dependencies)
                command = line.strip()
                commit = 'HEAD' #default
                state = 'description'
                parameters = {}
                dependencies = Set()
                print("command = " + command)  
            else:
                #Check if there are any parameters specified
                if line.find('=') != -1:
                    index = 1
                    while line.find('=', index) != -1:
                        pos = line.find('=', index)
                        comma = line.find(',', pos)#This will work for numbers and strings without commas
                        if comma != -1:
                            if line.find('[', pos,comma):#Deal with lists
                                openBracket = line.find('[', pos,comma)
                                closeBracket = line.find(']', openBracket+1)
                                comma = line.find(',', closeBracket)
                            parameters[line[index:pos].strip()] = eval(line[pos+1:comma])
                            index = comma+1
                        else:
                            newline = line.find('\n', pos)
                            parameters[line[index:pos].strip()] = eval(line[pos+1:newline])
                            break
                    #print parameters
                else:
                    #print("dependency stage")
                    #Check for tab, if no tab then the experiement description is finished.
                    if line[0:1] != '\t':
                        print("Finish " + command)
                        check_dependencies(parameters,desc,commit, command,None,dependencies)
                        command = line.strip()
                        commit = 'HEAD' #default
                        state = 'description'
                        parameters = {}
                        dependencies = Set()
                        print("command = " + command)  
                    else:
                        index = 2
                        pos = line.find(',', index)
                        if pos == -1:
                            pos = line.find('}', index)
                        if line.find('[all]',index,pos) != -1:
                            pos = pos - 5
                            all = True
                        else:
                            all = False
                        while pos != -1:
                            #print("index = " + str(index))
                            #print("pos = " + str(pos))
                            if line[index] == '"':
                                d = eval(line[index:pos]).strip()
                                if d in nodes:
                                    dependencies.add((d,all))
                                else:
                                    print('Error in line ' + str(count) + ' of ' + args.file + ':All dependencies must have been previously definied.')
                                    exit(-1)
                            else:                                    
                                param = line[index:pos]
                                #print(index)
                                #print(pos)
                                #print(param)
                                if param in parameters and isinstance(parameters[param],str):
                                    d = parameters[param]
                                    if d in nodes:
                                        dependencies.add((d,all))
                                    else:
                                        print('Error in line ' + str(count) + ' of ' + args.file + ': All dependencies must have been previously definied.')
                                        exit(-1)
                                else:
                                    print('Error in line ' + str(count) + ' of ' + args.file + '(' + line + '): Dependencies should be either descriptions of other experiments enclosed in quotation marks or names of string parameters defined for this experiment.\n')
                                    exit(-1)
                            if line[pos] == '}' or (all and line[pos+5] == '}'):
                                break
                            index = pos+1
                            pos = line.find(',', index)
                            if pos == -1:
                                pos = line.find('}', index)
   # print("Finish " + command + " (desc = " + desc + ")")
    check_dependencies(parameters,desc,commit, command,None,dependencies)

    toplevel_nodes = []
    for nodeGroup in nodes:
        for node in nodes[nodeGroup]:
            if not node.parents:
                toplevel_nodes += [node]

    mydag = dag(toplevel_nodes)
    mydag.backend = local_backend.local_backend()
    mydag.mainloop()

    
def printDag():
    print("The following is the structure of the dag...")
    for nodeGroup in nodes:
        print("Description = " + nodeGroup)
        for node in nodes[nodeGroup]:
            print("\tAddress = ")
            print(node)
            print("\tCommit = " + node.commit)
            print("\tCommand = " + node.command)
            print("\tParams = ")
            print(node.params)
            print("\tParents = ")
            print(node.parents)
            
       

#Need to check if any of the dependencies are to experiements that were run for a list of parameter values
def check_dependencies(parameters,desc,commit,command,dependencies,dependencies_to_search):
    if len(dependencies_to_search) == 0:
        #print("Calling check_parameters parameters = ")
        #print parameters
        #print("dependencies = ")
        #print dependencies
        check_parameters({},parameters,desc,commit,command,dependencies)
        return
    this_dependencies_to_search = dependencies_to_search.copy()
    (dep,all) = this_dependencies_to_search.pop()
    if dependencies == None:
        this_dependencies = Set()
    else:
        this_dependencies = dependecies.copy()
    if len(nodes[dep]) == 1: #all=True here doens't make sense so don't check for it
        #Don't need to worry about creating multiple nodes for this dependency
        #Add the actual node, not just name
        for d in  nodes[dep]:
            this_dependencies.add(d)
        check_dependencies(parameters, desc, commit, command, this_dependencies, this_dependencies_to_search)
    else:
        if all:
            print("Consolidating becaues of an 'all'")
            print(nodes[dep])
            this_dependencies = this_dependencies.union(nodes[dep])
            print this_dependencies
            check_dependencies(parameters,desc,commit,command, this_dependencies, this_dependencies_to_search)
        else:
            for d in nodes[dep]:
                this_dependencies.add(d)
                check_dependencies(parameters,desc,commit,command, this_dependencies,this_dependencies_to_search)
                this_dependencies.remove(d)

def check_parameters(parameters_searched, parameters_to_search, desc, commit, command, dependencies):
    this_parameters_to_search = copy.deepcopy(parameters_to_search)
    if len(parameters_to_search) != 0:
        #cur_params_searched = parameters_searched.copy()
        #cur_params_to_search = parameters_to_search.copy()
        #for key in parameters_to_search:
        (key,value) = this_parameters_to_search.popitem()
        if isinstance(value, list):
            #Need to create at least one node for each element in list. May have to create more if other
            #parameters are also lists.
            #print("value")
            #print(value)
            for l in value:
                #print("l = " + str(l))
                this_parameters_searched = copy.deepcopy(parameters_searched)
                this_parameters_searched[key] = l
                #print cur_params_searched
                #print cur_params_to_search
                check_parameters(this_parameters_searched, this_parameters_to_search, desc,commit,command,dependencies)
        else:
            this_parameters_searched = copy.deepcopy(parameters_searched)
            this_parameters_searched[key] = value
            check_parameters(this_parameters_searched, this_parameters_to_search, desc,commit,command,dependencies)
    else:
     #   print("ready to make node, parameters = ")
     #   print(parameters_searched)
        newNode = dag_node(desc,parameters_searched,commit,command, None, dependencies)
    #    print("newNode = " )
    #    print newNode
    #    print("parameters = ")
    #    print(newNode.params)
        if desc in nodes:
            nodes[desc].add(newNode)
        else:
            nodes[desc] = Set()
            nodes[desc].add(newNode)
        #Add to all nodes in dependencies that they have a child nodes[desc]
        #print("dependencies = ")
        #print dependencies
        newNodeSet = Set()
        newNodeSet.add(newNode)
        if dependencies != None:
            for d in dependencies:
                d.add_children(newNodeSet)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run experiements with dependencies described in a file and track content created by code')
    subparsers = parser.add_subparsers()
    
    runfile = subparsers.add_parser('runfile', help='run all the experiements described in a file')
    runfile.add_argument('file', help='file from which all experiments should be run')
    runfile.set_defaults(func=run_file)
    
    args = parser.parse_args()
    args.func(args)
