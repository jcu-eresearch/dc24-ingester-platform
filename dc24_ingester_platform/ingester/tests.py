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
from dc24_ingester_platform.service import IIngesterService
from dc24_ingester_platform.ingester import IngesterEngine
from dc24_ingester_platform.ingester.data_sources import DataSource

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

class MockService(IIngesterService):
    def __init__(self):
        self.logs = {}
        
    def getDataSourceState(self, dataset_id):
        return {}
    
    def persistDataSourceState(self, dataset_id, state):
        return
    
    def logIngesterEvent(self, dataset_id, timestamp, level, message):
        if dataset_id not in self.logs:
            self.logs[dataset_id] = []
        self.logs[dataset_id].append( (timestamp, level, message) )

class MockSource(DataSource):
    def __init__(self, source, state):
        self.source = source
        self.state = state
        

class MockSourceCSV1(MockSource):
    """This simple data source will create a CSV file with 2 lines"""
    def fetch(self, cwd):
        with open(os.path.join(cwd, "file"), "w") as f:
            f.write("0,1\n1,2\n")
            
        return {"time":format_timestamp(datetime.datetime.now()), "file":"file"}

class TestIngesterProcess(unittest.TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp()
        
        self.service = MockService()
        self.ingester = IngesterEngine(self.service, self.data_source_factory)

    def tearDown(self):
        shutil.rmtree(self.cwd)

    def data_source_factory(self, source, state):
        if source["class"] == "csv1":
            return MockSourceCSV1(source, state)
        
    def testBasicIngest(self):
        """This test performs a simple data ingest"""
        dataset = {"id":1, "data_source":{"class":"csv1"}}
        self.ingester.queue(dataset)
        self.ingester.processQueue()
        
        self.assertEquals(1, len(self.ingester._ingest_queue))
        
    def testComplexIngest(self):
        """This test performs a simple data ingest"""
        script = """import os
import datetime
from dc24_ingester_platform.utils import *

def process(cwd, data_entry):
    ret = [data_entry]
    with open(os.path.join(cwd, data_entry["file"])) as f:
        for l in f.readlines():
            l = l.strip().split(",")
            if len(l) != 2: continue
            ret.append({"time":format_timestamp(datetime.datetime.now()), "a":l[1].strip()})
    return ret
"""            
        dataset = {"id":1, "data_source":{"class":"csv1"}, "processing_script":script}
        self.ingester.queue(dataset)
        self.ingester.processQueue()
        
        self.assertEquals(3, len(self.ingester._ingest_queue))
        
                