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
import shutil
import Queue

from processor import *
from dc24_ingester_platform.utils import *
from twisted.internet.task import LoopingCall
from dc24_ingester_platform.ingester.sampling import create_sampler
from dc24_ingester_platform.ingester.data_sources import create_data_source
import traceback

logger = logging.getLogger("dc24_ingester_platform")

class IngesterEngine(object):
    def __init__(self, service, staging_dir, data_source_factory):
        """Create an ingester engine, and register itself with the service facade.
        """
        self.service = service
        self.service.register_observation_listener(self)
        self.staging_dir = staging_dir
        if not os.path.exists(self.staging_dir): os.makedirs(self.staging_dir)
        self._queue = Queue.Queue()
        self._ingest_queue = Queue.Queue()
        self._data_source_factory = data_source_factory
        self.running = True
        
    def shutdown(self):
        """Signal that we want to shutdown to our threads"""
        self.running = False
        
    def processSamplers(self):
        """Process all the active dataset samplers to determine which are
        firing. Only one copy of this method will ever be running at a time.
        """
        now = datetime.datetime.now()
        datasets = self.service.getActiveDatasets()
        logger.info("Got %s datasets at %s"%(len(datasets), str(now)))
        # Verify if the schedule has run
        for dataset in datasets:
            if dataset.running: continue
            if not hasattr(dataset.data_source, "sampling") or dataset.data_source.sampling == None: continue
            self.processSampler(now, dataset)

        #self.processQueue()

    def processSampler(self, now, dataset):
        """To process a dataset:
        1. load the sampler state
        2. Call the sampler
        3. If the sampler returns False, save the sampler state and exit
        4. If it returns True, mark the dataset as running, queue the dataset to run
        """
        state = self.service.getSamplerState(dataset.id)
        try:
            sampler = create_sampler(dataset.data_source.sampling, state)
            self.service.persistSamplerState(dataset.id, sampler.state)
            if sampler.sample(now, dataset):
                self.queue(dataset)
        except Exception, e:
            logger.error("DATASET.id=%d: %s"%(dataset.id,str(e)))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def processQueue(self):
        """Process the pending fetch and process queue"""
        while self.running:
            try:
                dataset, parameters = self._queue.get(True, 5)
            except Queue.Empty:
                # just loop, checking the running flag
                continue
            
            state = self.service.getDataSourceState(dataset.id)
            try:
                data_source = self._data_source_factory(dataset.data_source, state, parameters)
                self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "INFO", "Processing ")
                cwd = tempfile.mkdtemp(dir=self.staging_dir)
                
                data_entries = data_source.fetch(cwd, self.service)
                
                if hasattr(data_source, "processing_script") and data_source.processing_script != None:
                    data_entries = run_script(data_source.processing_script, cwd, data_entries)
                
                for entry in data_entries:
                    entry.dataset = dataset.id
                self.queueIngest(data_entries, cwd)
                
                self.service.persistDataSourceState(dataset.id, data_source.state)
                self.service.markNotRunning(dataset.id)
                
            except Exception, e:
                logger.error("DATASET.id=%d: %s"%(dataset.id,str(e)))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback)
                self.service.logIngesterEvent(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def processIngestQueue(self):
        """Process one entry in the ingest queue. 
        Each element in the queue may be many data entries, from the same data source.
        """
        while self.running:
            try:
                entries, cwd = self._ingest_queue.get(True, 5)
            except Queue.Empty:
                continue
    
            for entry in entries:
                self.service.persist(entry, cwd)
            # Cleanup
            shutil.rmtree(cwd)
  
    def queue(self, dataset, parameters=None):
        """Enqueue the dataset for fetch and process ASAP"""
        self.service.markRunning(dataset.id)
        self._queue.put( (dataset, parameters) )

    def queueIngest(self, ingest_data, cwd):
        """Queue a data entry for ingest into the repository"""
        self._ingest_queue.put((ingest_data, cwd))
        
    def notifyNewObservation(self, obs, cwd):
        """Notification of new data. On return it is expected that the cwd will be
        cleaned up, so any data that is required should be copied.
        """
        datasets = self.service.getActiveDatasets(kind="dataset_data_source")
        datasets = [ds for ds in datasets if ds.data_source.dataset_id==obs.dataset]
        logger.info("Notified of new observation, telling %d listeners"%(len(datasets)))
        for dataset in datasets:
            self.queue(dataset, {"dataset":obs.dataset, "id":obs.id})
        

def startIngester(service, staging_dir, data_source_factory=create_data_source):
    """Setup and start the ingester loop.
    
    :param service: the service facade
    :param staging_dir: the folder that will hold all the staging data
    """
    ingester = IngesterEngine(service, staging_dir, data_source_factory)
    lc = LoopingCall(ingester.processSamplers)
    lc.start(15, False)
    
#    lc = LoopingCall(ingester.processIngestQueue)
#    lc.start(15, False)
    
    from twisted.internet import reactor
    reactor.callInThread(ingester.processQueue)
    reactor.callInThread(ingester.processIngestQueue)
    reactor.addSystemEventTrigger("before", "shutdown", lambda : ingester.shutdown())

    return ingester
