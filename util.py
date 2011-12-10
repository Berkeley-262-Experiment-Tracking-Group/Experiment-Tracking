import subprocess
import os
import time


def exec_output(args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]
def abs_root_path():
    return exec_output(['git', 'rev-parse', '--show-toplevel']).strip()
def save_descr(path, info):
    """Save info about an experiment to a file

    info is intended to be a dictionary of objects for which repr does the
    right thing"""

    with open(path, 'w') as f:
        f.write(repr(info))
        f.write('\n')

