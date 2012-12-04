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
import tempfile
from processor import *

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
    return [data_entry, None, None]
"""
        new_entries = run_script(script, self.cwd, data_entry)

        self.assertEquals(3, len(new_entries))

    @unittest.skip("Only valid with sandbox")
    def testFileAccess(self):
        script = """def process(cwd, data_entry):
    import os
    return os.listdir(".")
"""
        self.assertRaises(ImportError, run_script, (script, self.cwd, None))

