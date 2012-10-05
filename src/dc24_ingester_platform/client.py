"""
Ingest Platform management client

This module will contain a CLI client for the management interface XMLRPC service. It
also contains all the data transfer objects.

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""
import sys
import xmlrpclib
import getopt

"""This is a base class to allow easy type checking
"""
class BaseManagementObject(object):
    @property
    def id(self):
        return self._id
    @id.setter
    def id(self, value):
        self._id = value
        
    def __init__(self):
        self._id = None
        
class Dataset(BaseManagementObject):
    @property
    def latitude(self):
        return self._latitude
    @latitude.setter
    def latitude(self, value):
        self._latitude = value
    @property
    def longitude(self):
        return self._longitude
    @longitude.setter
    def longitude(self, value):
        self._longitude = value
        
    def __init__(self):
        self._id = None
        self._latitude = None
        self._longitude = None


CLASSES = {str(Dataset):"dataset"}
CLASS_FACTORIES = {"dataset": Dataset}

def obj_to_dict(obj):
    """Maps an object of base class BaseManagementObject to a dict.
    """
    if not CLASSES.has_key(str(obj.__class__)):
        raise ValueError("This object class is not supported")
    ret = {}
    ret["class"] = CLASSES[str(obj.__class__)]
    for attr in dir(obj):
        if attr.startswith("_"): continue
        ret[attr] = getattr(obj, attr)
    return ret

def dict_to_obj(x):
    """Maps a dict back to an object, created based on the 'class' element.
    """
    if not x.has_key("class"):
        raise ValueError("There is no class element")
    obj = CLASS_FACTORIES[x["class"]]()
    for k in x:
        setattr(obj, k, x[k])
    return obj

class Client(object):
    """Initialise the client connection using the given URL
    @param server_url: The server URL. HTTP and HTTPS only.
    
    >>> s = Client("")
    Traceback (most recent call last):
    ...
    ValueError: Invalid server URL specified
    >>> s = Client("ssh://")
    Traceback (most recent call last):
    ...
    ValueError: Invalid server URL specified
    >>> c = Client("http://localhost:8080")
    """
    def __init__(self, server_url):
        if not server_url.startswith("http://") and not server_url.startswith("https://"):
            raise ValueError("Invalid server URL specified")
        self.server = xmlrpclib.ServerProxy(server_url, allow_none=True)
        
    """Create a new entry using the passed in object, the entry type will be based on the objects type.
    
    @ingester_object - If the objects ID is set an exception will be thrown.
    @return the object passed in with the ID field set.
    
    >>> c = Client("http://localhost:8080")
    
    """
    def insert(self, ingester_object):
        if ingester_object.id != None:
            raise ValueError("The object should not have an ID set")
        # Prepare the object for transmission
        object_dict = obj_to_dict(ingester_object)
        return dict_to_obj(self.server.insert(object_dict))

    """
        Update an entry using the passed in object, the entry type will be based on the objects type.
    
        @ingester_object - If the passed in object doesn't have it's ID set an exception will be thrown.
        @return the updated object (eg. @return == ingester_object should always be true on success).
    """
    def update(self, ingester_object):
        if ingester_object.id == None:
            raise ValueError("The object should have an ID set")
        # Prepare the object for transmission
        object_dict = obj_to_dict(ingester_object)
        return dict_to_obj(self.server.update(object_dict))

    """
        Delete an entry using the passed in object, the entry type will be based on the objects type.
    
        @ingester_object - All fields except the objects ID will be ignored.
        @return the object that has been deleted, this should have all fields set.
    """
    def delete(self, ingester_object):
        pass

    """
        Get an entry using the passed in object, the entry type will be based on the objects type.
    
        @ingester_object - An ingester object with either the ID or any combination of other fields set
        @return -   If the objects ID field is set an object of the correct type that matches the ID
                        will be returned.
                    If the object ID field isn't set an array of all objects of the correct type that
                        match the set fields will be returned.
    """
    def get(self, ingester_object):
        pass

    """
        Get all ingester logs for a single dataset.
    
        @dataset_id - ID of the dataset to get ingester logs for
        @return an array of file handles for all log files for that dataset.
    """
    def get_ingester_logs(self, dataset_id):
        pass

def main():
    """Main entry point. Exposed as a function so that it is compatible with the egg script process"""
    opts, args = getopt.getopt(sys.argv[1:], "s:")
    server_url = None
    for k,v in opts:
        if k == "-s":
            server_url = v
    
    errors = ""
    if server_url == None:
        errors += "No server URL set"
        
    if len(errors) > 0:
        print errors
        sys.exit(1)

if __name__ == "__main__":
    main()
        
    