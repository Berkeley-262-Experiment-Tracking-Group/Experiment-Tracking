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

from sets import Set
import argparse
import copy
import shutil
import os
import exp_common

import util, dag, local_backend

nodes = {}



def parse_file(filename):
    f = open(filename, 'r');
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
           # print("command = " + command)

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
           #     print("command = " + command)  
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
                                if(comma==-1):
                                    comma=closeBracket+1
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
                   #     print("Finish " + command)
                        check_dependencies(parameters,desc,commit, command,None,dependencies)
                        command = line.strip()
                        commit = 'HEAD' #default
                        state = 'description'
                        parameters = {}
                        dependencies = Set()
                    #    print("command = " + command)  
                    else:
                        index = 2
                        pos = line.find(',', index)
                        if pos == -1:
                            pos = line.find('}', index)
                        
                        while pos != -1:
                            if line.find('[all]',index,pos) != -1:
                                pos = pos - 5
                                all = True
                            else:
                                all = False
                            #print(line[index:pos])
                            #print("index = " + str(index))
                            #print("pos = " + str(pos))
                            if line.find('"', index, pos) != -1:
                                startQuote = line.find('"', index)
                                d = eval(line[startQuote:pos]).strip()
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
                            if all:
                                index = index+5
                            pos = line.find(',', index)
                            if pos == -1:
                                pos = line.find('}', index)
   # print("Finish " + command + " (desc = " + desc + ") dependencies = ")
   # print dependencies
    check_dependencies(parameters,desc,commit, command,None,dependencies)

    
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
        this_dependencies = dependencies.copy()
    if len(nodes[dep]) == 1: #all=True here doens't make sense so don't check for it
        #Don't need to worry about creating multiple nodes for this dependency
        #Add the actual node, not just name
        for d in  nodes[dep]:
            this_dependencies.add(d)
        check_dependencies(parameters, desc, commit, command, this_dependencies, this_dependencies_to_search)
    else:
        if all:
          #  print("Consolidating becaues of an 'all'")
           # print(nodes[dep])
            this_dependencies = this_dependencies.union(nodes[dep])
           # print this_dependencies
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
        
        #if command starts with @, it is a macro
        if(command[0]=='@'):
            code=command[1:]
            newNode = dag.dag_node(desc,parameters_searched,commit,None, code, dependencies)
        else:
            newNode = dag.dag_node(desc,parameters_searched,commit,command, None, dependencies)
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
 


###### Functions to save the 'task'

# Functions to fill in a 'HEAD' commit in all nodes    
def fill_in_commit(new_commit):
    for nodeGroup in nodes:
        for node in nodes[nodeGroup]:
             if(node.commit=='HEAD'):
                 node.commit=new_commit

# Run a dag
def run():
    toplevel_nodes = []
    for nodeGroup in nodes:
        for node in nodes[nodeGroup]:
            if not node.parents:
                toplevel_nodes += [node]

    mydag = dag.dag(toplevel_nodes)
    mydag.backend = local_backend.local_backend()
    status = mydag.mainloop()
    if status == dag.RUN_STATE_SUCCESS:
        print "Task completed successfully."
    elif status == dag.RUN_STATE_FAIL:
        print "Task failed!"
    else:
        print "Unrecognized exit status"

# Save the task. Creates a directory containing the file and another file containing the commit.
def save_task(filename, commit):
    task_id=1
    rootdir=util.abs_root_path()
    taskdir=os.path.join(rootdir, exp_common.TASK_DIR)
    if not os.path.isdir(taskdir):
        if not os.path.isdir(exp_common.DOT_DIR):
            os.mkdir(exp_common.DOT_DIR)
        os.mkdir(taskdir)
    else:
        a=[int(x) for x in os.listdir(taskdir)];
        task_id=max(a)+1
    try:    
        os.mkdir(os.path.join(taskdir, str(task_id)))
    except:
        print 'Could not create task directory. Aborting'
        exit(1)
        
    
    shutil.copy(filename, os.path.join(taskdir, str(task_id)))
    new_filename=os.path.join(taskdir, str(task_id),filename)
    task_namespace=dict()
    task_namespace['commit']=commit
    task_namespace['filename']=filename
    print "task file is", os.path.join(taskdir, str(task_id), exp_common.TASK_COMMIT_FILE)
    with open(os.path.join(taskdir, str(task_id), exp_common.TASK_COMMIT_FILE),'w') as f:
        f.write(repr(task_namespace))
        f.write('\n')
        
    return task_id

# Loads a particular task
def load_task(task_id):
    rootdir=util.abs_root_path()
    taskdir=os.path.join(rootdir, exp_common.TASK_DIR)
    taskfilename=os.path.join(taskdir, str(task_id), exp_common.TASK_COMMIT_FILE)
    try:
        f=open(taskfilename)
    except:
        print 'Could not access task file {}'.format(taskfilename)
        exit(1)
    task_namespace=eval(f.read())
    return (task_namespace['filename'], task_namespace['commit'])

# Run a file
def run_file(args):
    filename=args.file
    
    #Parse the file 
    parse_file(filename)
    
    # Get the current commit hash
    commit=util.exec_output(['git', 'rev-parse', 'HEAD']).strip()
    
    # Fill in this commit wherever HEAD occurs
    
    fill_in_commit(commit)
   
    # Create a new task
    task_id=save_task(filename, commit)
    print 'The id for this task is {}'.format(str(task_id))
   
    # Start running
    run()

# Run an old task
def run_old_task(args):
    task_id=int(args.taskid)
    
    # Load the task
    (filename, commit)=load_task(task_id)
    
    # Parse the file
    parse_file(filename)
    
    #Fill in commit
    fill_in_commit(commit)
    run()

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run experiements with dependencies described in a file and track content created by code')
    subparsers = parser.add_subparsers()
    
    runfile = subparsers.add_parser('runfile', help='run all the experiements described in a file')
    runfile.add_argument('file', help='file from which all experiments should be run')
    runfile.set_defaults(func=run_file)
    
    runtask = subparsers.add_parser('runtask', help='run all the experiements from an old task')
    runtask.add_argument('taskid', help='id of the task')
    runtask.set_defaults(func=run_old_task)
    
    args = parser.parse_args()
    args.func(args)
