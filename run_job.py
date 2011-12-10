import dag, util, local_backend
import time

# this file is just for testing; if a user actually wanted to run a
# job they'd use exp.py.

hsh = util.exec_output(['git', 'rev-parse', 'HEAD']).strip()

test_node = dag.dag_node(desc = "testscript", commit = hsh, command = "./test.sh")
test_node2=dag.dag_node(desc = "testscript2", commit = hsh, command = "./test2.sh {testscript}/log")

test_node2.add_parents(set([test_node,]));
test_dag = dag.dag([test_node,])
lb = local_backend.local_backend()
test_dag.backend = lb

test_dag.mainloop()
