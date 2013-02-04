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
import logging
from processor import *
from dc24_ingester_platform.service import IIngesterService
from dc24_ingester_platform.ingester import IngesterEngine, create_data_source
from dc24_ingester_platform.ingester.data_sources import DataSource
from jcudc24ingesterapi.models.data_entry import DataEntry, FileObject
from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.models.data_sources import _DataSource

logger = logging.getLogger("dc24_ingester_platform")

class TestScriptModels(unittest.TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cwd)

    def testScript(self):
        file1 = "1\n2\n"
        with open(os.path.join(self.cwd, "file1"), "w") as f:
            f.write(file1)
        data_entry = DataEntry(timestamp=datetime.datetime.now())
        data_entry["file1"] = FileObject("file1")

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
        self.datasets = {}
        
    def getDataSourceState(self, dataset_id):
        return {}
    
    def persistDataSourceState(self, dataset_id, state):
        return
    
    def getDataset(self, dataset_id):
        return self.datasets[dataset_id]
    
    def logIngesterEvent(self, dataset_id, timestamp, level, message):
        if dataset_id not in self.logs:
            self.logs[dataset_id] = []
        self.logs[dataset_id].append( (timestamp, level, message) )

class MockSource(DataSource):
    pass

class MockSourceCSV1(MockSource):
    """This simple data source will create a CSV file with 2 lines"""
    def fetch(self, cwd):
        with open(os.path.join(cwd, "file"), "w") as f:
            f.write("2,55\n3,2\n")
            
        data_entry = DataEntry(timestamp=datetime.datetime.now())
        data_entry["file1"] = FileObject("file1")
        
        return [data_entry]

class TestIngesterProcess(unittest.TestCase):
    def setUp(self):
        self.cwd = tempfile.mkdtemp()
        self.staging = tempfile.mkdtemp()
        self.todelete = []
        
        self.service = MockService()
        self.ingester = IngesterEngine(self.service, self.staging, self.data_source_factory)

    def tearDown(self):
        shutil.rmtree(self.cwd)
        shutil.rmtree(self.staging)
        for d_name in self.todelete:
            shutil.rmtree(d_name)

    def data_source_factory(self, source, state, parameters):
        if source.__xmlrpc_class__ == "csv1":
            args = source.__dict__
            return MockSourceCSV1(state, parameters, **args)
        else:
            return create_data_source(source, state, parameters)
        
    def testBasicIngest(self):
        """This test performs a simple data ingest"""
        dataset = Dataset()
        dataset.id = 1
        datasource = _DataSource()
        datasource.__xmlrpc_class__ = "csv1"
        
        dataset.data_source = datasource
        
        self.ingester.queue(dataset)
        self.ingester.processQueue()
        
        self.assertEquals(1, len(self.ingester._ingest_queue))
        
    def testComplexIngest(self):
        """This test performs a complex data ingest, where the main data goes into dataset 1 and 
        the extracted data goes into dataset 2"""
        script = """import os
import datetime
from dc24_ingester_platform.utils import *

def process(cwd, data_entry):
    data_entry = data_entry[0]
    ret = [data_entry]
    with open(os.path.join(cwd, data_entry["file"]["path"])) as f:
        for l in f.readlines():
            l = l.strip().split(",")
            if len(l) != 2: continue
            ret.append( (2,{"timestamp":format_timestamp(datetime.datetime.now()), "a":{"path":l[1].strip()}}) )
    return ret
"""            
        dataset = {"id":1, "data_source":{"class":"csv1", "processing_script":script}}
        dataset2 = {"id":2}
        self.service.datasets[1] = dataset
        self.service.datasets[2] = dataset2
        
        self.ingester.queue(dataset)
        self.ingester.processQueue()
        self.assertEquals(3, len(self.ingester._ingest_queue))
        
    def testPush(self):
        """This tests the push ingest by creating a test dir, populating it, then forcing the ingester to run
        """
        staging = tempfile.mkdtemp()
        self.todelete.append(staging)
        
        # Create a temp file to ingest
        f = open(os.path.join(staging, str(int(time.time()))), "a")
        f.close()
        
        # Check there is only 1 file here
        self.assertEquals(1, len(os.listdir(staging)))
        
        dataset = {"id":1, "data_source":{"class":"push_data_source"}}
        self.ingester.queue(dataset, {"path":staging})
        self.ingester.processQueue()
        
        self.assertEquals(1, len(self.ingester._ingest_queue))
        
        # Check there are now no files
        self.assertEquals(0, len(os.listdir(staging)))
        
