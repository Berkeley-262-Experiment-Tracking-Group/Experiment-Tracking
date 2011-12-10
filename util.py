import subprocess
import os
import time
import hashlib


# Shortcuts for running shell commands

def exec_cmd(args):
    p = subprocess.Popen(args)
    return os.waitpid(p.pid, 0)[1]

def exec_shell(cmd):
    p = subprocess.Popen(cmd, shell=True)
    return os.waitpid(p.pid, 0)[1]

def exec_output(args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]

def abs_root_path():
    return exec_output(['git', 'rev-parse', '--show-toplevel']).strip()

def sha1(s):
    return hashlib.sha1(s).hexdigest()
