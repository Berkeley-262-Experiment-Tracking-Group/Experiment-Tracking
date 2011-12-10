#!/usr/bin/env python
import os
import subprocess
import dag, util
import time

class local_backend:

    def __init__(self):
        pass

    def run(self, node):
        
        # go to the experimental directory
        os.chdir(os.path.join(node.expdir, node.working_dir))

        # run the experiment
        print 'Running command ' + node.new_cmd + ' in directory ' + os.getcwd()
        
        node.jobid = subprocess.Popen(node.new_cmd, shell = True)
        return node.jobid

    def get_state(self, node):
        if node.info['run_state'] != dag.RUN_STATE_RUNNING:
            return node.info['run_state']
        else:
            return_code = node.jobid.poll()
            if return_code is None:
                return dag.RUN_STATE_RUNNING, return_code
            elif return_code == 0:
                print "Command '%s' exited with status %d." \
                    % (node.new_cmd, return_code)
                node.info['date_end'] = time.time()
                return dag.RUN_STATE_SUCCESS, return_code            
            else:
                print "Command '%s' exited with status %d" \
                    % (node.new_cmd, return_code)
                return dag.RUN_STATE_FAIL, return_code


