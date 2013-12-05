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
import traceback

from processor import *
from dc24_ingester_platform.utils import *
from twisted.internet.task import LoopingCall
from dc24_ingester_platform.ingester.sampling import create_sampler
from dc24_ingester_platform.ingester.data_sources import create_data_source
from jcudc24ingesterapi.ingester_platform_api import Marshaller
from jcudc24ingesterapi.models.data_sources import DatasetDataSource
from jcudc24ingesterapi.ingester_exceptions import InvalidObjectError, OperationFailedException

logger = logging.getLogger("dc24_ingester_platfor.ingester")

class IngesterEngine(object):
    def __init__(self, service, staging_dir, data_source_factory):
        """Create an ingester engine, and register itself with the service facade.
        """
        self.service = service
        self.service.register_observation_listener(self)
        self.staging_dir = staging_dir
        if not os.path.exists(self.staging_dir): os.makedirs(self.staging_dir)
        self._ingress_queue = Queue.Queue()
        self._archive_queue = Queue.Queue()
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
                self.enqueue_ingress(dataset)
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
                dataset, parameters, task_id, cwd = self._ingress_queue.get(True, 5)
            except Queue.Empty:
                # just loop, checking the running flag
                continue
            
            state = self.service.get_data_source_state(dataset.id)
            try:
                data_source = self._data_source_factory(dataset.data_source, state, parameters)
                self.service.log_ingester_event(dataset.id, datetime.datetime.now(), "INFO", "Processing ")
                
                data_entries = data_source.fetch(cwd, self.service)
                
                if len(data_entries) > 0:
                    if hasattr(data_source, "processing_script") and data_source.processing_script != None:
                        data_entries = run_script(data_source.processing_script, cwd, data_entries)
                    
                    # Store the entries as a file on disk so that it is persistent during restarts
                    if isinstance(data_entries, list):
                        for entry in data_entries:
                            entry.dataset = dataset.id
                            
                        # Write entries to disk so we can recover later
                        entries_file = "ingest.json"
                        with open(os.path.join(cwd, entries_file), "w") as f:
                            json.dump(self.domain_marshaller.obj_to_dict(data_entries), f)
                        
                    else:
                        # Rename the output file to be consistent
                        shutil.move(data_entries, os.path.join(cwd, "ingest.json"))
                        entries_file = "ingest.json"
                    
                    # Now queue for ingest
                    self.enqueue_archive(task_id, entries_file, cwd)
                
                self.service.persist_data_source_state(dataset.id, data_source.state)
                self.service.mark_ingress_complete(task_id)
                
            except Exception, e:
                logger.error("DATASET.id=%d: %s"%(dataset.id,str(e)))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback)
                self.service.log_ingester_event(dataset.id, datetime.datetime.now(), "ERROR", str(e))
  
    def process_archive_queue(self, single_pass=False):
        """Process one entry in the ingest queue. 
        Each element in the queue may be many data entries, from the same data source.
        """
        running = True
        while (not single_pass and self.running) or (single_pass and running):
            if single_pass:
                running = False
            try:
                task_id, entries_file, cwd = self._archive_queue.get(True, 5)
            except Queue.Empty:
                continue
            try: 
                with open(os.path.join(cwd, entries_file), "r") as f:
                    entries = self.domain_marshaller.dict_to_obj(json.load(f))
                    
                for entry in entries:
                    self.service.persist(entry, cwd)
                # Cleanup
                self.service.mark_ingest_complete(task_id)
                shutil.rmtree(cwd)
            except Exception, e:
                logger.error("Error while archiving %d %s, not cleaning it up: %s"%(task_id, entries_file, str(e)))
                self.service.mark_ingest_failed(task_id)
  
    def enqueue_ingress(self, dataset, parameters=None):
        """Enqueue the dataset for ingress and processing ASAP. The markRunning method
        is assumed to also persist the queue entry. The queue_id is the identifier
        in the persistent store, so that the queued entry can be cleaned up."""
        cwd = tempfile.mkdtemp(dir=self.staging_dir)
        task_id = self.service.create_ingest_task(dataset.id, cwd, parameters)
        self._ingress_queue.put( (dataset, parameters, task_id, cwd) )

    def enqueue_archive(self, task_id, ingest_data, cwd):
        """Queue a data entry for ingest into the repository
        :param task_id: the ID given to the ingest process task
        :param ingest_data: the data entries to be ingested
        :param cwd: the working directory for these data entries
        """
        self._archive_queue.put((task_id, ingest_data, cwd))
        
    def notify_new_data_entry(self, data_entry, cwd):
        """Notification of new data. On return it is expected that the cwd will be
        cleaned up, so any data that is required should be copied.
        :param data_entry: DataEntry object that is triggering
        :param cwd: the working directory for the data entry
        """
        datasets = self.service.get_active_datasets(kind="dataset_data_source")
        datasets = [ds for ds in datasets if ds.data_source.dataset_id==data_entry.dataset]
        logger.info("Notified of new observation, telling %d listeners"%(len(datasets)))
        for dataset in datasets:
            self.enqueue_ingress(dataset, {"dataset":data_entry.dataset, "id":data_entry.id})
        
    def restore_running(self):
        """Load any persisted ingress and archive tasks"""
        items = self.service.get_ingest_queue()
        logger.info("Loading %d items into queue"%len(items))
        for task_id, state, dataset, parameters, cwd in items:
            if state == 0:
                # State 0 is ready to ingress
                self._ingress_queue.put( (dataset, parameters, task_id, cwd) )
            elif state == 1:
                # State 1 is ready to ingest
                try:
                    #with open(os.path.join(cwd, "ingest.json")) as f:
                    #    entries = self.domain_marshaller.dict_to_obj(json.load(f))
                    #    self.enqueue_archive(task_id, entries, cwd)
                    self.enqueue_archive(task_id, "ingest.json", cwd)
                except Exception as e:
                    logger.error("Error loading ingest task %d: %s"%(task_id, str(e)))
            else:
                logger.error("Unknown state %d for task %d"%(state, task_id))

    def invoke_ingester(self, dataset):
        """Invoke the specified ingester"""
        if not isinstance(dataset, Dataset):
            raise InvalidObjectError("The object is not a dataset")
        if dataset.data_source == None:
            raise InvalidObjectError("The provided dataset has no data source")
            
        if hasattr(dataset.data_source, "sampling") and dataset.data_source.sampling != None:
            # If the dataset has sampling then test if it is running and then run
            if dataset.running:
                raise OperationFailedException("The dataset is already running")
            self.enqueue_ingress(dataset)
        elif isinstance(dataset.data_source, DatasetDataSource):
            # If this is a dataset data source then enqueue all of the parent items            
            dataset_id = dataset.data_source.dataset_id
            data_entries =  self.service.find_data_entries(dataset_id=dataset_id)
            for data_entry in data_entries:
                self.enqueue(dataset, {"dataset":data_entry.dataset, "id":data_entry.id})
            
        else:
            raise OperationFailedException("The dataset has no ingester to run")

def start_ingester(service, staging_dir, data_source_factory=create_data_source):
    """Setup and start the ingester loop.
    
    :param service: the service facade
    :param staging_dir: the folder that will hold all the staging data
    """
    ingester = IngesterEngine(service, staging_dir, data_source_factory)
    ingester.restore_running()
    
    # Start the sampler loop
    lc = LoopingCall(ingester.process_samplers)
    lc.start(15, False)
    
    from twisted.internet import reactor
    reactor.callInThread(ingester.process_ingress_queue)
    reactor.callInThread(ingester.process_archive_queue)
    reactor.addSystemEventTrigger("before", "shutdown", lambda : ingester.shutdown())

    return ingester
