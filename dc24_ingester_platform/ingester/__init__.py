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

from twisted.internet.task import LoopingCall
from dc24_ingester_platform.ingester.sampling import create_sampler
from dc24_ingester_platform.ingester.data_sources import create_data_source

logger = logging.getLogger("dc24_ingester_platform.ingester")

class IngesterEngine(object):
    def __init__(self, service):
        self.service = service
        self._queue = []
        
    def runIngesters(self):
        now = datetime.datetime.now()
        datasets = self.service.ingester.getActiveDatasets()
        logger.info("Got %s datasets"%(len(datasets)))
        # Verify if the schedule has run
        for dataset in datasets:
            if "sampling" not in dataset or dataset["sampling"] == None: continue 
            self.processSampler(now, dataset)

        self.processQueue()

    def processQueue(self):
        while len(self._queue) > 0:
            next = self._queue[0]
            del self._queue[0]
            print next
            
            state = self.service.ingester.getDataSourceState(next["id"])
            try:
                data_source = create_data_source(next["data_source"], state)
                self.service.ingester.logIngesterEvent(next["id"], datetime.datetime.now(), "INFO", "Processing ")
                cwd = tempfile.mkdtemp()
                
                ingest_data = data_source.fetch(cwd)
                
                print cwd, ingest_data
                self.service.ingester.persistDataSourceState(next["id"], data_source.state)
            except Exception, e:
                print str(e)
                self.service.ingester.logIngesterEvent(next["id"], datetime.datetime.now(), "ERROR", str(e))

    def processSampler(self, now, dataset):
        """To process a dataset:
        1. load the sampler state
        2. Call the sampler
        3. If the sampler returns False, save the sampler state and exit
        4. If it returns True, queue the sample to run
        """
        state = self.service.ingester.getSamplerState(dataset["id"])
        try:
            sampler = create_sampler(dataset["sampling"], state)
            self.service.ingester.persistSamplerState(dataset["id"], sampler.state)
            if sampler.sample(now, dataset):
                self.queue(dataset)
        except Exception, e:
            self.service.ingester.logIngesterEvent(dataset["id"], datetime.datetime.now(), "ERROR", str(e))
    
    def queue(self, dataset):
        """Enqueue the dataset for ingestion ASAP"""
        self._queue.append(dataset)

def startIngester(service):
    ingester = IngesterEngine(service)
    lc = LoopingCall(ingester.runIngesters)
    lc.start(15, False)
    return ingester
