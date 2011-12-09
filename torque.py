
# Blackbox for Torque
# Untested to the extreme
from dag import dag_node
import os
import subprocess

default_options='-N {} -r n -e {} -o {} -d {} -lnodes=1:ppn=1' 
launch_command='qsub '

class Torque_blackbox:
    #write a bash cscript. Basically prints out exit status 
    def write_bash_script(self, filename, command):
	f=open(filename, 'w')
	f.write('#!/bin/bash\n')
	f.write('./' +command+'\n')
	f.write('echo  $?\n')
	f.close()

    def run(node, options=default_options):

	#Fill in options
	options2=options.format(node.hsh, os.path.join(node.exp_path, 'err'), os.path.join(node.exp_path, 'log'), os.path.join(node.expdir, node.working_dir))

	#Write bash script
	filename=os.path.join(node.expdir, node.hsh+'.sh')
	write_bash_script(filename, command)
	os.system('chmod 700 '+filename)

	#launch job
	qsub_command=launch_command+options2+filename
	pipe=subprocess.Popen(qsub_command, stdout=subprocess.PIPE)
	node.job_id=pipe.read().strip
 
    def getState(node):
        #The code that calls this will need to update the info file of the job.
	qstat_command='qstat | grep \'{}\''.format(node.job_id)
	canfind=os.system(qstat_command)
        if(not canfind):
            logfile=os.path.join(node.exp_path, 'log')
            with open(logfile) as f:
		status=list(f)[-1]
	    if(status == '0'):
	        return RUN_STATE_SUCCESS
	    else
		return RUN_STATE_FAILURE
	else
	    return RUN_STATE_RUNNING


