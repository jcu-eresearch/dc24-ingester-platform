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
        :param service: Service Facade instance being exposed by this XMLRPX service
        """
        xmlrpc.XMLRPC.__init__(self, allowNone=True)
        self.service = service
        
    def xmlrpc_insert(self, obj):
        """ Insert the passed object into the ingester platform
        """
        try:
            return self.service.ingester.persist(obj)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        
    def xmlrpc_update(self, obj):
        """Store the passed object.
        """
        try:
            return self.service.ingester.persist(obj)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
    

    def xmlrpc_commit(self, unit):
        """Commits a unit of work.
        """
        try:
            return self.service.ingester.commit(unit)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))

    def xmlrpc_getLocation(self, loc_id):
        """Retrieve a location by id
        """
        try:
            return self.service.ingester.getLocation(loc_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))

    def xmlrpc_getDataset(self, ds_id):
        """Retrieve a dataset by id
        """
        try:
            return self.service.ingester.getDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))

    def xmlrpc_enableDataset(self, ds_id):
        """Enable ingestion of a dataset.
        """
        try:
            return self.service.ingester.enableDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))

    def xmlrpc_disableDataset(self, ds_id):
        """Disable ingestion of a dataset.
        """
        try:
            return self.service.ingester.disableDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        
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
        self.service.ingester.reset()
        self.service.repository.reset()

def makeServer(service):
    """Construct a management service server using the supplied service facade.
    """
    return server.Site(ManagementService(service))

def makeResettableServer(service):
    """Construct a management service server using the supplied service facade.
    """
    return server.Site(ResettableManagementService(service))
