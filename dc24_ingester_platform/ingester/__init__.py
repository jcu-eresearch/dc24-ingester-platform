"""
Ingester scheduling service module.

This module will contain the scheduling and queuing logic for data retrieval, 
formatting, and ingestion.

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""

import logging
import datetime
import tempfile
import time
from processor import *
from dc24_ingester_platform.utils import *
from twisted.internet.task import LoopingCall
from dc24_ingester_platform.ingester.sampling import create_sampler
from dc24_ingester_platform.ingester.data_sources import create_data_source

logger = logging.getLogger("dc24_ingester_platform")

class IngesterEngine(object):
    def __init__(self, service, staging_dir, data_source_factory=create_data_source):
        """Create an ingester engine, and register itself with the service facade.
        """
        self.service = service
        self.service.register_observation_listener(self)
        self.staging_dir = staging_dir
        if not os.path.exists(self.staging_dir): os.makedirs(self.staging_dir)
        self._queue = []
        self._ingest_queue = []
        self._data_source_factory = data_source_factory
        
    def processSamplers(self):
        """Process all the active dataset samplers to determine which are
        firing.
        """
        now = datetime.datetime.now()
        datasets = self.service.getActiveDatasets()
        logger.info("Got %s datasets at %s"%(len(datasets), str(now)))
        # Verify if the schedule has run
        for dataset in datasets:
            if dataset.sampling == None: continue 
            self.processSampler(now, dataset)

        self.processQueue()

    def processSampler(self, now, dataset):
        """To process a dataset:
        1. load the sampler state
        2. Call the sampler
        3. If the sampler returns False, save the sampler state and exit
        4. If it returns True, queue the sample to run
        """
        state = self.service.getSamplerState(dataset.id)
        try:
            sampler = create_sampler(dataset.sampling, state)
            self.service.persistSamplerState(dataset.id, sampler.state)
            if sampler.sample(now, dataset):
                self.queue(dataset)
        except Exception, e:
            self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def processQueue(self):
        """Process the pending fetch and process queue"""
        while len(self._queue) > 0:
            dataset, parameters = self._queue[0]
            del self._queue[0]
            
            state = self.service.getDataSourceState(dataset.id)
            try:
                data_source = self._data_source_factory(dataset.data_source, state, parameters)
                self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "INFO", "Processing ")
                cwd = tempfile.mkdtemp(dir=self.staging_dir)
                
                data_entries = data_source.fetch(cwd)
                
                if hasattr(data_source, "processing_script") and data_source.processing_script != None:
                    data_entries = run_script(data_source.processing_script, cwd, data_entries)
                
                for entry in data_entries:
                    if isinstance(entry, tuple):
                        logger.info("storing to non-default dataset")
                        self.queueIngest(self.service.getDataset(entry[0]), entry[1], cwd)
                    else:
                        logger.info("storing to default dataset")
                        self.queueIngest(dataset, entry, cwd)
                        
                self.service.persistDataSourceState(dataset.id, data_source.state)
            except Exception, e:
                logger.error(str(e))
                self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def processIngestQueue(self):
        """Process one entry in the ingest queue"""
        if len(self._ingest_queue) == 0: return
        dataset, obs, cwd = self._ingest_queue[0]
        del self._ingest_queue[0]
        # FIXME
        self.service.persistObservation(dataset, obs.time, obs, cwd)
  
    def queue(self, dataset, parameters=None):
        """Enqueue the dataset for fetch and process ASAP"""
        self._queue.append( (dataset, parameters) )

    def queueIngest(self, dataset, ingest_data, cwd):
        """Queue a data entry for ingest into the repository"""
        self._ingest_queue.append((dataset, ingest_data, cwd))
        
    def notifyNewObservation(self, dataset, timestamp, attrs, cwd):
        """Notification of new data. On return it is expected that the cwd will be
        cleaned up, so any data that is required should be copied.
        """

def startIngester(service, staging_dir):
    """Setup and start the ingester loop.
    
    :param service: the service facade
    :param staging_dir: the folder that will hold all the staging data
    """
    ingester = IngesterEngine(service, staging_dir)
    lc = LoopingCall(ingester.processSamplers)
    lc.start(15, False)
    
    lc = LoopingCall(ingester.processIngestQueue)
    lc.start(15, False)
    
    return ingester
