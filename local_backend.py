import os
import subprocess
import dag, exp
import time

class lbackend:

    def __init__(self):
        pass

    def run(self, node):
        
        # go to the experimental directory
        os.chdir(os.path.join(node.expdir, node.working_dir))

        # run the experiment
        print 'Running command ' + node.new_cmd
        
        node.jobid = subprocess.Popen(node.new_cmd, shell = True)
        return node.jobid

    def get_state(self, node):
        if node.info['run_state'] != dag.RUN_STATE_RUNNING:
            return node.info['run_state']
        else:
            return_code = node.jobid.poll()
            print "polled return code ", return_code
            if return_code is None:
                return dag.RUN_STATE_RUNNING, return_code
            elif return_code == 0:
                return dag.RUN_STATE_SUCCESS, return_code
            else:
                return dag.RUN_STATE_FAIL, return_code


hsh = exp.exec_output(['git', 'rev-parse', 'HEAD']).strip()
test_node = dag.dag_node("test script", dict(), hsh, command = "./test.sh")
test_dag = dag.dag([test_node,])
lb = lbackend()
test_dag.backend = lb

while test_dag.finished_running() == dag.RUN_STATE_RUNNING:
    test_dag.run_runnable_jobs()
    time.sleep(1)
    test_dag.update_states()

