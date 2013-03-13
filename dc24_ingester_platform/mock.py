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
        time.sleep(10)
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
        
    def persistSamplerState(self, ds_id, state):
        self.sampler_state[ds_id] = state
    
    def getSamplerState(self, ds_id):
        return self.sampler_state[ds_id] if ds_id in self.sampler_state else {}

    def persistDataSourceState(self, ds_id, state):
        self.data_source_state[ds_id] = state

    def getDataSourceState(self, ds_id):
        return self.data_source_state[ds_id] if ds_id in self.data_source_state else {}
    
    def getActiveDatasets(self):
        return self.datasets
    
    def logIngesterEvent(self, dataset_id, timestamp, level, message):
        logger.info("[%s] %s"%(level, message))
        return
    
    def persist(self, entry, cwd):
        logger.info("Got entry: "+str(entry))

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

