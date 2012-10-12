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
    def __init__(self, service):
        """Initialise the management service. 
        @param service: Service Facade instance being exposed
        """
        xmlrpc.XMLRPC.__init__(self, allowNone=True)
        self.service = service
        
    def xmlrpc_insert(self, obj):
        """
        Return all passed args.
        """
        klass = obj["class"]
        del obj["class"]
        if klass == "dataset":
            return self.service.ingester.persistDataset(obj)
        elif klass == "location":
            return self.service.ingester.persistLocation(obj)
        else:
            raise xmlrpc.Fault(1, "%s not supported"%(obj["class"]))

    def xmlrpc_update(self, obj):
        """
        Return all passed args.
        """
        if obj["class"] == "dataset":
            return self.service.ingester.persistDataset(obj)
        else:
            raise xmlrpc.Fault("%s not supported"%(obj["class"]))
    
    def xmlrpc_ping(self):
        """A simple connection diagnostic method.
        """
        return "PONG"
        
    def xmlrpc_fault(self):
        """
        Raise a Fault indicating that the procedure should not be used.
        """
        raise xmlrpc.Fault(123, "The fault procedure is faulty.")

class ResettableManagementService(ManagementService):
    def __init__(self, *args, **kwargs):
        ManagementService.__init__(self, *args, **kwargs)

    def xmlrpc_reset(self):
        """Cleans out all data. Used only for testing
        """
        logger.info("Resetting data - Not Implemented")

def makeServer(service):
    """Construct a management service server using the supplied service facade.
    """
    return server.Site(ManagementService(service))

def makeResettableServer(service):
    """Construct a management service server using the supplied service facade.
    """
    return server.Site(ResettableManagementService(service))
