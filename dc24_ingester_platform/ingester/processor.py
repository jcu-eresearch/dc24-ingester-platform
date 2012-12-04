"""
This module contains code and tests relating to the processing of data using 
custom scripts.
"""
import os
import sys
import sandbox
from dc24_ingester_platform.utils import *


def create_sandbox(cwd):
    sb = sandbox.Sandbox()
    sb.config.allowPath(cwd)
    sb.config.enable("exit")
    return sb

def run_script(script, cwd, data_entry):
    """Runs the script provided (source code as string) and returns
    an array of additional data entries, including the original
    data_entry that may have been altered.
    """
    sb = create_sandbox(cwd)
    #return sb.call(_run_script, script, cwd, data_entry)
    return run_script_local(script, cwd, data_entry)

def _run_script(script, cwd, data_entry):
    code = compile(script, "<string>", "exec")
    exec(code)
    return process(cwd, data_entry)

def run_script_local(script, cwd, data_entry):
    code = compile(script, "<string>", "exec")
    local = {}
    exec(code, globals(), local)
    return local["process"](cwd, data_entry)


