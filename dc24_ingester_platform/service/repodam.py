"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import BaseRepositoryService, method
import decimal
import logging
import os
import shutil
import dam
import time
from jcudc24ingesterapi.models.data_entry import FileObject, DataEntry
from dc24_ingester_platform.utils import parse_timestamp

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
        

class RepositoryDAM(BaseRepositoryService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, url):
        self._url = url
        # A list of new obj ids that will be deleted on reset
        self.new_objs = []
    
    def connection(self):
        return dam.DAM(self._url)
    
    def reset(self):
        logger.info("Deleting items from the DAM")
        self.new_objs.reverse()
        with self.connection() as repo:
            repo.delete(self.new_objs)
        self.new_objs = []

    @method("persist", "schema")
    def persist_schema(self, schema):
        if hasattr(schema, "repository_id") and schema.repository_id != None:
            logger.error("Can't update schemas")
            return schema.repository_id
        
        attrs = [{"name":attr.name, "identifier":attr.name, "type":attr.kind} for attr in schema.attributes]
        for parent in schema.extends:
            attrs += [{"name":attr.name, "identifier":attr.name, "type":attr.kind} for attr in parent.attributes]

        for attr in attrs:
            if attr["type"] in ("integer", "double"):
                attr["type"] = "numerical"
        dam_schema = {"dam_type":"SchemaMetaData",
                "type":"DatasetMetaData",
                "name":schema.name if schema.name != None else "tdh_%d"%schema.id,
                "identifier":"tdh_%d"%schema.id,
                "attributes":attrs}
        with self.connection() as repo:
            dam_schema = repo.ingest(dam_schema)
        self.new_objs.append(dam_schema["id"])
        return dam_schema["id"]

    @method("persist", "location")
    def persist_location(self, location):
        dam_location = {"dam_type":"LocationMetaData",
            "name":location.name,
            "latitude":location.latitude,
            "longitude":location.longitude,
            "zones":[]}
        if hasattr(location, "repository_id") and location.repository_id != None:
            dam_location["id"] = location.repository_id
        with self.connection() as repo:
            dam_location = repo.ingest(dam_location)
        self.new_objs.append(dam_location["id"])
        return dam_location["id"]

    @method("persist", "dataset")
    def persist_dataset(self, dataset, schema, location):
        dam_dataset = {"dam_type":"DatasetMetaData",
            "location":location.repository_id,
            "zone":"",
            "schema":schema.repository_id}
        if hasattr(dataset, "repository_id") and dataset.repository_id != None:
            dam_dataset["id"] = dataset.repository_id
        with self.connection() as repo:
            dam_dataset = repo.ingest(dam_dataset)
        self.new_objs.append(dam_dataset["id"])
        return dam_dataset["id"]
    
    def _persist_attributes(self, obs, attributes, cwd):
        with self.connection() as repo:
            for attr_name in attributes: 
                attr = {"name":attr_name} # DAM Attribute
                if isinstance(attributes[attr_name], FileObject):
                    attr["originalFileName"] = attributes[attr_name].file_name
                    with open(os.path.join(cwd, attributes[attr_name].f_path), "rb") as f:
                        repo.ingest_attribute(obs["id"], attr, f)
                else:
                    attr["value"] = attributes[attr_name]
                    repo.ingest_attribute(obs["id"], attr)
    
    def persist_data_entry(self, dataset, schema, data_entry, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(data_entry.data, schema.attrs)
        
        with self.connection() as repo:
            dam_obs = {"dam_type":"ObservationMetaData",
                "dataset":dataset.repository_id,
                "time":dam.format_time(data_entry.timestamp)}
            
            dam_obs = repo.ingest(dam_obs, lock=True)
            
            self._persist_attributes(dam_obs, data_entry.data, cwd)
            
            repo.unlock(dam_obs["id"])
            self.new_objs.append(dam_obs["id"])
        
        return self.get_data_entry(dataset.id, dam_obs["id"])
        
    def get_data_entry(self, dataset_id, data_entry_id):
        with self.connection() as repo:
            dam_obj = repo.getTuples(data_entry_id)
        if dam_obj == None and len(dam_obj) == 1: return None
        dam_obj = dam_obj[0]
        data_entry = DataEntry()
        data_entry.id = data_entry_id
        data_entry.dataset = dataset_id
        data_entry.timestamp = parse_timestamp(dam_obj["metadata"]["time"])
        for attr in dam_obj["data"]:
            if "size" in attr:
                fo = FileObject()
                fo.f_name = attr["name"]
                fo.mime_type = attr["mimeType"]
                fo.file_name = attr["originalFileName"]
                data_entry.data[attr["name"]] = fo
            else:
                data_entry.data[attr["name"]] = attr["value"]
        return data_entry

    def get_data_entry_stream(self, dataset_id, data_entry_id, attr):
        repo = self.connection()
        return repo.retrieve_attribute(data_entry_id, attr, close_connection=True)
    
    
