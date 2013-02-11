"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IRepositoryService, method
import decimal
import logging
import os
import shutil
import dam
import time

logger = logging.getLogger(__name__)

def obj_to_dict(obj, klass=None):
    """Maps an object of base class BaseManagementObject to a dict.
    """
    ret = {}
    for attr in dir(obj):
        if attr.startswith("_") or attr == "metadata": continue
        if type(getattr(obj, attr)) in (str, int, float, unicode, dict):
            ret[attr] = getattr(obj, attr)
        elif type(getattr(obj, attr)) == decimal.Decimal:
            ret[attr] = float(getattr(obj, attr))
    if klass != None: ret["class"] = klass
    elif hasattr(obj, "__xmlrpc_class__"): ret["class"] = obj.__xmlrpc_class__
    return ret

def dict_to_object(dic, obj):
    for attr in dir(obj):
        if attr.startswith("_"): continue
        if dic.has_key(attr): setattr(obj, attr, dic[attr])

def merge_parameters(col_orig, col_new, klass, name_attr="name", value_attr="value"):
    """This method updates col_orig removing any that aren't in col_new, updating those that are, and adding new ones
    using klass as the constructor
    
    col_new is a dict
    col_orig is a list
    klass is a type
    """
    working = col_new.copy()
    to_del = []
    for obj in col_orig:
        if getattr(obj,name_attr) in working:
            # Update
            setattr(obj, value_attr, working[obj.name])
            del working[obj.name]
        else:
            # Delete pending
            to_del.append(obj)
    # Delete
    for obj in to_del:
        col_orig.remove(obj)
    # Add
    for k in working:
        obj = klass()
        setattr(obj, name_attr, k)
        setattr(obj, value_attr, working[k])
        col_orig.append(obj)
        

class RepositoryDAM(IRepositoryService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, url):
        self.repo = dam.DAM(url)
        # A list of new obj ids that will be deleted on reset
        self.new_objs = []
    
    def reset(self):
        logger.info("Deleting items from the DAM")
        self.repo.delete(self.new_objs)

    @method("persist", "schema")
    def persistSchema(self, schema):
        attrs = [{"name":attr.name, "identifier":attr.name, "type":attr.kind} for attr in schema.attributes]
        dam_schema = {"dam_type":"SchemaMetaData",
                "type":"DatasetMetaData",
                "name":str(time.time()),
                "identifier":str(int(time.time())),
                "attributes":attrs}
        dam_schema = self.repo.ingest(dam_schema)
        self.new_objs.append(dam_schema["id"])
        return dam_schema["id"]

    @method("persist", "location")
    def persistLocation(self, location):
        dam_location = {"dam_type":"LocationMetaData",
            "name":location.name,
            "latitude":location.latitude,
            "longitude":location.longitude,
            "zones":[]}
        dam_location = self.repo.ingest(dam_location)
        self.new_objs.append(dam_location["id"])
        return dam_location["id"]

    @method("persist", "dataset")
    def persistDataset(self, dataset, schema, location):
        dam_dataset = {"dam_type":"DatasetMetaData",
            "location":location.repositoryId,
            "zone":"",
            "schema":schema.repositoryId}
        dam_dataset = self.repo.ingest(dam_dataset)
        self.new_objs.append(dam_dataset["id"])
        return dam_dataset["id"]
    
    def persistObservation(self, dataset, schema, timestamp, attrs, cwd):
        schema = schema["attributes"]
        # Check the attributes are actually in the schema
        for k in attrs:
            if k not in schema:
                raise ValueError("%s is not in the schema"%(k))
        
        dam_obs = {"dam_type":"ObservationMetaData",
            "dataset":dataset["repositoryId"],
            "time":dam.format_time(timestamp)}
        dam_obs = self.repo.ingest(dam_obs, lock=True)
        for k in attrs:
            attr = {"name":k}
            if schema[k] == "file":
                f = open(os.path.join(cwd, attrs[k]), "rb")
                self.repo.ingest_attribute(dam_obs["id"], attr, f)
                f.close()
            else:
                attr["value"] = attrs[k]
                self.repo.ingest_attribute(dam_obs["id"], attr)
        self.repo.unlock(dam_obs["id"])
        self.new_objs.append(dam_obs["id"])

