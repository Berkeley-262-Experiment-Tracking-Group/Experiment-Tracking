#!/usr/bin/env python
import os
import subprocess
import dag, util
import time
import sys

class local_backend:

    # write a bash cscript. Basically prints out exit status 
    def write_bash_script(self, filename, command, cwd):
    	f=open(filename, 'w')
	f.write('#!/bin/bash\n')
	f.write('/usr/bin/env PATH=$PATH:'+cwd+' ' +command+'\n')
	f.write('echo $?\n')
	f.close()

    def __init__(self):
        pass

    def run(self, node):
        
        # go to the experimental directory
        os.chdir(os.path.join(node.expdir, node.working_dir))

        # Write bash script
	filename=os.path.join(node.expdir, node.hsh+'.sh')
	self.write_bash_script(filename, node.new_cmd, os.getcwd())
        run_command = (filename + ' | tee %s/log 2>&1') % (node.exp_results)

	os.system('chmod 700 '+filename)

        # run the experiment
        print 'Running command ' + node.new_cmd + ' in directory ' + os.getcwd()
        
        node.jobid = subprocess.Popen(run_command, shell=True)
        return node.jobid

    def get_state(self, node):
        if node.info['run_state'] != dag.RUN_STATE_RUNNING:
            return node.info['run_state']
        else:
            return_code = node.jobid.poll()

            if return_code is None:
                return dag.RUN_STATE_RUNNING, return_code

            logfile=os.path.join(node.exp_results, 'log')
            with open(logfile) as f:
		status=int(list(f)[-1])
            if status == 0:
                print "Command '%s' exited with status %d." \
                    % (node.new_cmd, status)
                node.info['date_end'] = time.time()
                return dag.RUN_STATE_SUCCESS, status            
            else:
                print "Command '%s' exited with status %d" \
                    % (node.new_cmd, status)
                return dag.RUN_STATE_FAIL, status


