"""
Management Service XMLRPC server

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""

from twisted.web import xmlrpc
import logging
import os
import shutil
from dc24_ingester_platform.service import PersistenceError
from twisted.web.resource import Resource

logger = logging.getLogger(__name__)

class ManagementService(xmlrpc.XMLRPC):
    """
    An example object to be published.
    """
    def __init__(self, staging_dir, service):
        """Initialise the management service. 
        :param service: Service Facade instance being exposed by this XMLRPX service
        """
        xmlrpc.XMLRPC.__init__(self, allowNone=True)
        self.service = service
        self.transaction_counter = 0
        if not os.path.exists(staging_dir):
            raise ValueError("The provided staging directory doesn't exist")
        self.transactions = {}
        self.staging_dir = staging_dir
        
    def xmlrpc_insert(self, obj):
        """ Insert the passed object into the ingester platform
        """
        try:
            return self.service.persist(obj)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 
        
    def xmlrpc_update(self, obj):
        """Store the passed object.
        """
        try:
            return self.service.persist(obj)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e))
        
    def xmlrpc_precommit(self, unit):
        """Creates a staging area for a unit of work and returns the transaction ID
        """
        try:
            # Fix me, this is a possible race condition in a multithreaded env
            transaction_id = self.transaction_counter
            cwd = os.path.join(self.staging_dir, str(transaction_id))
            if not os.path.exists(cwd):
                os.mkdir(cwd)
                
            self.transaction_counter += 1
            self.transactions[transaction_id] = cwd, unit
            return transaction_id
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e))

    def xmlrpc_commit(self, transaction_id):
        """Commits a unit of work based on the transaction ID.
        """
        try:
            cwd, unit = self.transactions[int(transaction_id)]
            return self.service.commit(unit, cwd)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e))
        finally:
            self.cleanup_transaction(transaction_id)

    def xmlrpc_getLocation(self, loc_id):
        """Retrieve a location by id
        """
        try:
            return self.service.getLocation(loc_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 

    def xmlrpc_getDataset(self, ds_id):
        """Retrieve a dataset by id
        """
        try:
            return self.service.getDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 

    def xmlrpc_enableDataset(self, ds_id):
        """Enable ingestion of a dataset.
        """
        try:
            return self.service.enableDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 

    def xmlrpc_disableDataset(self, ds_id):
        """Disable ingestion of a dataset.
        """
        try:
            return self.service.disableDataset(ds_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 
        
    def xmlrpc_findDatasets(self, search_args):
        """Disable ingestion of a dataset.
        """
        try:
            return self.service.findDatasets(**search_args)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
            raise xmlrpc.Fault(1, str(e)) 
        
    def xmlrpc_runIngester(self, d_id):
        """Disable ingestion of a dataset.
        """
        try:
            return self.service.runIngester(d_id)
        except ValueError, e:
            raise xmlrpc.Fault(1, str(e))
        except PersistenceError, e:
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
    
    def cleanup_transaction(self, transaction_id):
        """Clean up all transaction files"""
        shutil.rmtree(self.transactions[transaction_id][0])
        del self.transactions[transaction_id]
        

class ResettableManagementService(ManagementService):
    def __init__(self, *args, **kwargs):
        ManagementService.__init__(self, *args, **kwargs)

    def xmlrpc_reset(self):
        """Cleans out all data. Used only for testing
        """
        self.service.reset()

class DataController(Resource):
    isLeaf = True

    def __init__(self, service, xmlrpc):
        Resource.__init__(self)
        self.service = service
        self.xmlrpc = xmlrpc

    def render_HEAD(self, request):
        if len(request.postpath) == 0:
            return self.xmlrpc.render_HEAD(request)
        else:
            return Resource.render_HEAD(self, request)

    def render_POST(self, request):
        """On post get the ingest key from the path.
        Then, store the post body for ingest.
        """
        # If this is the root then dispatch to the XMLRPC server
        if len(request.postpath) == 0:
            return self.xmlrpc.render_POST(request)
        
        if len(request.postpath) != 3:
            request.setResponseCode(400)
            return "Invalid request"
        transaction_id = request.postpath[0]
        obj_id = request.postpath[1] # <object class>-<object id>
        attr = request.postpath[2]
        
        if not int(transaction_id) in self.xmlrpc.transactions:
            request.setResponseCode(400)
            return "Transaction not found"
        
        transaction_path, unit = self.xmlrpc.transactions[int(transaction_id)]
        obj_path = os.path.join(transaction_path, obj_id)
        if not os.path.exists(obj_path):
            os.mkdir(obj_path)
        attr_rel_path = os.path.join(obj_id, attr)
        attr_path = os.path.join(obj_path, attr)
        with open(attr_path, "wb") as f:
            shutil.copyfileobj(request.content, f)
        
        class_, oid = obj_id.split(":")
        
        # Update the path
        done = False
        for sets in ["update", "insert"]:
            for item in unit[sets]:
                if class_ == item["class"] and int(oid) == item["id"]:
                    item["data"][attr]["path"] = attr_rel_path
                    done = True
                    break
            if done: break
        return "OK"

def makeServer(staging_dir, service):
    """Construct a management service server using the supplied service facade.
    """
    return DataController(service, ManagementService(staging_dir, service))

def makeResettableServer(staging_dir, service):
    """Construct a management service server using the supplied service facade.
    """
    return DataController(service, ResettableManagementService(staging_dir, service))
