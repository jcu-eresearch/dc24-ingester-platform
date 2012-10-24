"""
Ingester scheduling service module.

This module will contain the scheduling and queuing logic for data retrieval, 
formatting, and ingestion.

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""

import logging
from twisted.internet.task import LoopingCall

logger = logging.getLogger("dc24_ingester_platform.ingester")

class IngesterEngine(object):
    def __init__(self, service):
        self.service = service
        
    def runIngesters(self):
        logger.info("Ding - This is where we will process the pending ingester samples")
        datasets = self.service.ingester.getActiveDatasets()
        logger.info("Got %s datasets"%(len(datasets)))
        # Verify if the schedule has run
        for dataset in datasets:
            print dataset

def startIngester(service):
    ingester = IngesterEngine(service)
    lc = LoopingCall(ingester.runIngesters)
    lc.start(15, False)
    return ingester
