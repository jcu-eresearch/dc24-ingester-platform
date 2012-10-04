"""
Management Service XMLRPC server

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""

from twisted.web import xmlrpc, server
import logging

logger = logging.getLogger(__name__)

class ManagementService(xmlrpc.XMLRPC):
    """
    An example object to be published.
    """
    def __init__(self):
        xmlrpc.XMLRPC.__init__(self, allowNone=True)
    def xmlrpc_insert(self, x):
        """
        Return all passed args.
        """
        logger.info(x)
        return x

    def xmlrpc_add(self, a, b):
        """
        Return sum of arguments.
        """
        return a + b

    def xmlrpc_fault(self):
        """
        Raise a Fault indicating that the procedure should not be used.
        """
        raise xmlrpc.Fault(123, "The fault procedure is faulty.")

def makeServer():
    return server.Site(ManagementService())
