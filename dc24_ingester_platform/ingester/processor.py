"""
This module contains code and tests relating to the processing of data using 
custom scripts.
"""
import os
import sys
import sandbox
import unittest
import datetime
import shutil
from dc24_ingester_platform.utils import *


def create_sandbox(cwd):
    sandbox = sandbox.Sandbox()
    sandbox.config.allowPath(cwd)
    return sandbox

def run_script(script, cwd, data_entry):
    """Runs the script provided (source code as string) and returns
    an array of additional data entries, including the original
    data_entry that may have been altered.
    """
    code = compile(script, "<string>", "exec")
    exec code
    return process(cwd, data_entry)

class TestScriptModels(unittest.TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cwd)

    def testScript(self):
        file1 = "1\n2\n"
        with open(os.path.join(self.cwd, "file1"), "w") as f:
            f.write(file1)
        data_entry = {"time":format_timestamp(datetime.datetime.now()), "file1":"file1"}

        script = """def process(cwd, data_entry):
    return [data_entry]
"""

        new_entries = run_script(script, self.cwd, data_entry)

        self.assertEquals(3, len(new_entries))

