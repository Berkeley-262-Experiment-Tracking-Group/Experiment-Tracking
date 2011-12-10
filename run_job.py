import dag, util, local_backend
import time

hsh = util.exec_output(['git', 'rev-parse', 'HEAD']).strip()

test_node = dag.dag_node("testscript", dict(), hsh, command = "./test.sh")
test_node2=dag.dag_node("testscript2", dict(), hsh, command = "./test2.sh {testscript}/log")

test_node2.add_parents(set([test_node,]));
test_dag = dag.dag([test_node,])
lb = local_backend.local_backend()
test_dag.backend = lb

while test_dag.finished_running() == dag.RUN_STATE_RUNNING:
    test_dag.run_runnable_jobs()
    time.sleep(1)
    test_dag.update_states()

