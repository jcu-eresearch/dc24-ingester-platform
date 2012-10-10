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

def check_for_ingesters():
    logger.info("Ding!")

def startIngester():
    lc = LoopingCall(check_for_ingesters)
    lc.start(15, False)

