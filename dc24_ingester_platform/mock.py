"""
This module contains a mock service setup for testing/developing the ingester engine.

Created on Mar 13, 2013

@author: nigel
"""

import logging
import time
import datetime

from twisted.web import xmlrpc

from dc24_ingester_platform.ingester.data_sources import DataSource
from dc24_ingester_platform.service import IIngesterService
from jcudc24ingesterapi.models.dataset import Dataset
from jcudc24ingesterapi.models.data_sources import _DataSource
from jcudc24ingesterapi.models.sampling import PeriodicSampling
from jcudc24ingesterapi.models.data_entry import DataEntry

logger = logging.getLogger(__name__)

class MockDataSource(DataSource):
    def fetch(self, cwd, service=None):
        time.sleep(20)
        return [DataEntry(timestamp=datetime.datetime.now())]
        
def data_source_factory(data_source_config, state, parameters):
    return MockDataSource(state, parameters, data_source_config)

class MockService(IIngesterService):
    def __init__(self):
        self.obs_listeners = []
        self.data_source_state = {}
        self.sampler_state = {}
        
        self.datasets = []
        self.setup_datasets()
        
    def setup_datasets(self):
        ds = Dataset()
        ds.data_source = _DataSource()
        ds.id = 1
        ds.data_source.__xmlrpc_class__ = "mock_data_source"
        ds.data_source.sampling = PeriodicSampling(10000)
        self.datasets.append(ds)
        
    def register_observation_listener(self, listener):
        self.obs_listeners.append(listener)
        
    def unregister_observation_listener(self, listener):
        self.obs_listeners.remove(listener)
        
    def persist_sampler_state(self, ds_id, state):
        self.sampler_state[ds_id] = state
    
    def get_sampler_state(self, ds_id):
        return self.sampler_state[ds_id] if ds_id in self.sampler_state else {}

    def persist_data_source_state(self, ds_id, state):
        self.data_source_state[ds_id] = state

    def get_data_source_state(self, ds_id):
        return self.data_source_state[ds_id] if ds_id in self.data_source_state else {}
    
    def get_active_datasets(self):
        return self.datasets
    
    def log_ingester_event(self, dataset_id, timestamp, level, message):
        logger.info("[%s] %s"%(level, message))
        return
    
    def persist(self, entry, cwd):
        logger.info("Got entry: "+str(entry))
        
    def create_ingest_task(self, ds_id, params, cwd):
        self.datasets[0].running = True
        return 0
        
    def mark_ingress_complete(self, task_id):
        self.datasets[0].running = False

    def mark_ingest_complete(self, task_id):
        pass
        
class MockServer(xmlrpc.XMLRPC):
    def __init__(self, service):
        """Initialise the management service. 
        :param service: Service Facade instance being exposed by this XMLRPC service
        """
        xmlrpc.XMLRPC.__init__(self, allowNone=True)
        self.service = service

def makeMockService():
    return MockService()

def makeMockServer(service):
    return MockServer(service)

