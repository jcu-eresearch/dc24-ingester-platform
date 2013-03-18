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
import json
import Queue

from processor import *
from dc24_ingester_platform.utils import *
from twisted.internet.task import LoopingCall
from dc24_ingester_platform.ingester.sampling import create_sampler
from dc24_ingester_platform.ingester.data_sources import create_data_source
from jcudc24ingesterapi.ingester_platform_api import Marshaller
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
        self.domain_marshaller = Marshaller()
        
    def shutdown(self):
        """Signal that we want to shutdown to our threads"""
        self.running = False
        
    def process_samplers(self):
        """Process all the active dataset samplers to determine which are
        firing. Only one copy of this method will ever be running at a time.
        """
        now = datetime.datetime.now()
        datasets = self.service.get_active_datasets()
        logger.info("Got %s datasets at %s"%(len(datasets), str(now)))
        # Verify if the schedule has run
        for dataset in datasets:
            if dataset.running: continue
            if not hasattr(dataset.data_source, "sampling") or dataset.data_source.sampling == None: continue
            self.process_sampler(now, dataset)

        #self.processQueue()

    def process_sampler(self, now, dataset):
        """To process a dataset:
        1. load the sampler state
        2. Call the sampler
        3. If the sampler returns False, save the sampler state and exit
        4. If it returns True, mark the dataset as running, queue the dataset to run
        """
        state = self.service.get_sampler_state(dataset.id)
        try:
            sampler = create_sampler(dataset.data_source.sampling, state)
            self.service.persist_sampler_state(dataset.id, sampler.state)
            if sampler.sample(now, dataset):
                self.enqueue(dataset)
        except Exception, e:
            logger.error("DATASET.id=%d: %s"%(dataset.id,str(e)))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            self.service.log_ingester_event(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def process_ingress_queue(self, single_pass=False):
        """Process the pending ingress (fetch) and process queue"""
        running = True
        while (not single_pass and self.running) or (single_pass and running):
            if single_pass:
                running = False
            try:
                dataset, parameters, task_id, cwd = self._queue.get(True, 5)
            except Queue.Empty:
                # just loop, checking the running flag
                continue
            
            state = self.service.get_data_source_state(dataset.id)
            try:
                data_source = self._data_source_factory(dataset.data_source, state, parameters)
                self.service.log_ingester_event(dataset.id, datetime.datetime.now(), "INFO", "Processing ")
                
                data_entries = data_source.fetch(cwd, self.service)
                
                if hasattr(data_source, "processing_script") and data_source.processing_script != None:
                    data_entries = run_script(data_source.processing_script, cwd, data_entries)
                
                for entry in data_entries:
                    entry.dataset = dataset.id
                # Write entries to disk so we can recover later
                with open(os.path.join(cwd, "ingest.json"), "w") as f:
                    json.dump(self.domain_marshaller.obj_to_dict(data_entries), f)

                # Now queue for ingest
                self.enqueue_ingest(task_id, data_entries, cwd)
                
                self.service.persist_data_source_state(dataset.id, data_source.state)
                self.service.mark_ingress_complete(task_id)
                
            except Exception, e:
                logger.error("DATASET.id=%d: %s"%(dataset.id,str(e)))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback)
                self.service.log_ingester_event(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def process_ingest_queue(self, single_pass=False):
        """Process one entry in the ingest queue. 
        Each element in the queue may be many data entries, from the same data source.
        """
        running = True
        while (not single_pass and self.running) or (single_pass and running):
            if single_pass:
                running = False
            try:
                task_id, entries, cwd = self._ingest_queue.get(True, 5)
            except Queue.Empty:
                continue
    
            for entry in entries:
                self.service.persist(entry, cwd)
            # Cleanup
            self.service.mark_ingest_complete(task_id)
            shutil.rmtree(cwd)
  
    def enqueue(self, dataset, parameters=None):
        """Enqueue the dataset for ingress and processing ASAP. The markRunning method
        is assumed to also persist the queue entry. The queue_id is the identifier
        in the persistent store, so that the queued entry can be cleaned up."""
        cwd = tempfile.mkdtemp(dir=self.staging_dir)
        task_id = self.service.create_ingest_task(dataset.id, cwd, parameters)
        self._queue.put( (dataset, parameters, task_id, cwd) )

    def enqueue_ingest(self, task_id, ingest_data, cwd):
        """Queue a data entry for ingest into the repository
        :param task_id: the ID given to the ingest process task
        :param ingest_data: the data entries to be ingested
        :param cwd: the working directory for these data entries
        """
        self._ingest_queue.put((task_id, ingest_data, cwd))
        
    def notify_new_data_entry(self, obs, cwd):
        """Notification of new data. On return it is expected that the cwd will be
        cleaned up, so any data that is required should be copied.
        :param obs: DataEntry object that is triggering
        :param cwd: the working directory for the data entry
        """
        datasets = self.service.get_active_datasets(kind="dataset_data_source")
        datasets = [ds for ds in datasets if ds.data_source.dataset_id==obs.dataset]
        logger.info("Notified of new observation, telling %d listeners"%(len(datasets)))
        for dataset in datasets:
            self.enqueue(dataset, {"dataset":obs.dataset, "id":obs.id})
        
    def load_running(self):
        """Load any persisted ingress and ingest tasks"""
        items = self.service.get_ingest_queue()
        logger.info("Loading %d items into queue"%len(items))
        for task_id, state, dataset, parameters, cwd in items:
            if state == 0:
                # State 0 is ready to ingest
                self._queue.put( (dataset, parameters, task_id, cwd) )
            elif state == 1:
                # State 1 is ready to ingest
                try:
                    with open(os.path.join(cwd, "ingest.json")) as f:
                        entries = self.domain_marshaller.dict_to_obj(json.load(f))
                        self.enqueue_ingest(task_id, entries, cwd)
                except Exception as e:
                    logger.error("Error loading ingest task %d: %s"%(task_id, str(e)))
            else:
                logger.error("Unknown state %d for task %d"%(state, task_id))

def start_ingester(service, staging_dir, data_source_factory=create_data_source):
    """Setup and start the ingester loop.
    
    :param service: the service facade
    :param staging_dir: the folder that will hold all the staging data
    """
    ingester = IngesterEngine(service, staging_dir, data_source_factory)
    ingester.load_running()
    
    # Start the sampler loop
    lc = LoopingCall(ingester.process_samplers)
    lc.start(15, False)
    
    from twisted.internet import reactor
    reactor.callInThread(ingester.process_ingress_queue)
    reactor.callInThread(ingester.process_ingest_queue)
    reactor.addSystemEventTrigger("before", "shutdown", lambda : ingester.shutdown())

    return ingester
