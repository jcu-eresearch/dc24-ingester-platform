"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import BaseRepositoryService, method,\
    ingesterdb
import decimal
import logging
import os
import shutil
import dam
import time
from jcudc24ingesterapi.models.data_entry import FileObject, DataEntry
from dc24_ingester_platform.utils import parse_timestamp
from jcudc24ingesterapi.ingester_exceptions import UnknownObjectError, PersistenceError
from jcudc24ingesterapi.models.metadata import DatasetMetadataEntry, DataEntryMetadataEntry

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
        return dam.DAM(self._url, version="1.2")
    
    def reset(self):
        logger.info("Deleting items from the DAM")
        self.new_objs.reverse()
        with self.connection() as repo:
            repo.delete(self.new_objs)
        self.new_objs = []
        
    def mark_for_reset(self, oid):
        if oid not in self.new_objs: 
            self.new_objs.append(oid)

    @method("persist", "schema")
    def persist_schema(self, schema):
        try:
            if hasattr(schema, "repository_id") and schema.repository_id != None:
                logger.error("Can't update schemas")
                return schema.repository_id
            
            attrs = [{"name":attr.name, "identifier":attr.name, "type":attr.kind} for attr in schema.attributes]
            parents = [p.repository_id for p in schema.extends]
    
            for attr in attrs:
                if attr["type"] in ("integer", "double"):
                    attr["type"] = "numerical"
            dam_schema = {"dam_type":"Schema",
                    "type":"Dataset",
                    "name":schema.name if schema.name != None else "tdh_%d"%schema.id,
                    "identifier":"tdh_%d"%schema.id,
                    "attributes":attrs,
                    "extends": parents}
            with self.connection() as repo:
                dam_schema = repo.ingest(dam_schema)
            self.mark_for_reset(dam_schema["id"])
            return dam_schema["id"]
        except dam.DuplicateEntityException as e:
            logger.exception("Exception while persisting schema")
            raise PersistenceError("Error persisting schema: DuplicateEntityException %s"%(str(e)))
        except dam.DAMException as e:
            logger.exception("Exception while persisting schema")
            raise PersistenceError("Error persisting schema: %s"%(str(e)))

    @method("persist", "location")
    def persist_location(self, location):
        try:
            dam_location = {"dam_type":"Location",
                "name":location.name,
                "latitude":location.latitude,
                "longitude":location.longitude,
                "zones":[]}
            if hasattr(location, "repository_id") and location.repository_id != None:
                dam_location["id"] = location.repository_id
            with self.connection() as repo:
                dam_location = repo.ingest(dam_location)
            self.mark_for_reset(dam_location["id"])
            return dam_location["id"]
        except dam.DuplicateEntityException as e:
            logger.exception("Exception while persisting location")
            raise PersistenceError("Error persisting location: DuplicateEntityException %s"%(str(e)))
        except dam.DAMException as e:
            logger.exception("Exception while persisting location")
            raise PersistenceError("Error persisting location: %s"%(str(e)))

    @method("persist", "dataset")
    def persist_dataset(self, dataset, schema, location):
        try:
            dam_dataset = {"dam_type":"Dataset",
                "location":location.repository_id,
                "zone":"",
                "schema":schema.repository_id}
            if hasattr(dataset, "repository_id") and dataset.repository_id != None:
                dam_dataset["id"] = dataset.repository_id
            with self.connection() as repo:
                dam_dataset = repo.ingest(dam_dataset)
            self.mark_for_reset(dam_dataset["id"])
            return dam_dataset["id"]
        except dam.DuplicateEntityException as e:
            logger.exception("Exception while persisting dataset")
            raise PersistenceError("Error persisting dataset: DuplicateEntityException %s"%(str(e)))
        except dam.DAMException as e:
            logger.exception("Exception while persisting dataset")
            raise PersistenceError("Error persisting dataset: %s"%(str(e)))
    
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
        
        try:
            with self.connection() as repo:
                dam_obs = {"dam_type":"Observation",
                    "dataset":dataset.repository_id,
                    "time":dam.format_time(data_entry.timestamp)}
                
                dam_obs = repo.ingest(dam_obs, lock=True)
                
                self._persist_attributes(dam_obs, data_entry.data, cwd)
                
                repo.unlock(dam_obs["id"])
                self.mark_for_reset(dam_obs["id"])
            
            return self.get_data_entry(dataset.id, dam_obs["id"])
        except dam.DuplicateEntityException as e:
            logger.exception("Exception while persisting data entry")
            raise PersistenceError("Error persisting data entry: DuplicateEntityException %s"%(str(e)))
        except dam.DAMException as e:
            logger.exception("Exception while persisting data entry")
            raise PersistenceError("Error persisting data entry: %s"%(str(e)))
        
    def get_data_entry(self, dataset_id, data_entry_id):
        try:
            with self.connection() as repo:
                dam_obj = repo.getTuples(data_entry_id)
        except dam.DAMException as e:
            logger.exception("Exception while getting data entry")
            raise PersistenceError("Error getting data entry: %s"%(str(e)))
        
        if dam_obj == None and len(dam_obj) == 1: return None
        dam_obj = dam_obj[0]
        data_entry = DataEntry()
        data_entry.id = data_entry_id
        data_entry.dataset = dataset_id
        data_entry.timestamp = parse_timestamp(dam_obj["metadata"]["time"])
        self._copy_attrs(dam_obj["data"], data_entry)
        return data_entry

    def _copy_attrs(self, attrs, dst):
        for attr in attrs:
            if "size" in attr:
                fo = FileObject()
                fo.f_name = attr["name"]
                fo.mime_type = attr["mimeType"]
                fo.file_name = attr["originalFileName"]
                dst.data[attr["name"]] = fo
            else:
                dst.data[attr["name"]] = attr["value"]

    def get_data_entry_stream(self, dataset_id, data_entry_id, attr):
        repo = self.connection()
        return repo.retrieve_attribute(data_entry_id, attr, close_connection=True)
    
    def find_data_entries(self, dataset, limit=None, start_time=None, end_time=None):
        try:
            with self.connection() as repo:
                start_time = dam.format_time(start_time) if start_time is not None else None
                end_time = dam.format_time(end_time) if end_time is not None else None
                dam_objs = repo.retrieve_tuples("data", dataset=dataset.repository_id, 
                                limit=limit, startTime=start_time, endTime=end_time)
        except dam.DAMException as e:
            logger.exception("Exception while getting data entries")
            raise PersistenceError("Error getting data entries: %s"%(str(e)))
        
        ret = []
        for dam_obj in dam_objs:
            data_entry = DataEntry()
            data_entry.id = dam_obj["metadata"]["id"]
            data_entry.dataset = dataset.id
            data_entry.timestamp = parse_timestamp(dam_obj["metadata"]["time"])
            self._copy_attrs(dam_obj["data"], data_entry)
            ret.append(data_entry)
        return ret

    
    def persist_data_entry_metadata(self, dataset, schema, attrs, cwd):
        return self._persist_metadata(dataset, schema, attrs, cwd)
    
    def persist_dataset_metadata(self, dataset, schema, attrs, cwd):
        return self._persist_metadata(dataset, schema, attrs, cwd)
    
    def _persist_metadata(self, dataset, schema, attrs, cwd):
        """Persist object metadata returning the ID of the new object"""
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        
        try:
            with self.connection() as repo:
                obj_md = {"dam_type":"ObjectMetaData",
                    "subject":dataset.repository_id,
                    "schema":schema.repository_id}
                
                obj_md = repo.ingest(obj_md, lock=True)
                
                self._persist_attributes(obj_md, attrs, cwd)
                
                repo.unlock(obj_md["id"])
                self.mark_for_reset(obj_md["id"])
        except dam.DuplicateEntityException as e:
            logger.exception("Exception while persisting object metadata")
            raise PersistenceError("Error persisting object metadata: DuplicateEntityException %s"%(str(e)))
        except dam.DAMException as e:
            logger.exception("Exception while persisting object metadata")
            raise PersistenceError("Error persisting object metadata: %s"%(str(e)))
        
        return self._retrieve_object_metadata(obj_md["id"])
    
    def _retrieve_object_metadata(self, md_id):
        try:
            with self.connection() as repo:
                obj_md = repo.getTuples(md_id)[0]
                subject = repo.get(obj_md["metadata"]["subject"])
                
                schemas = self.service.find_schemas(repository_id = obj_md["metadata"]["schema"])
                if len(schemas) != 1:
                    for schema in schemas:
                        logger.error("Schema to repo mapping %d -> %s"%(schema.id, schema.repository_id))
                    raise UnknownObjectError("Expected 1 schema for repository ID %s, got %d"%(obj_md["metadata"]["schema"], len(schemas)))
                schema = schemas[0]
                
                if subject["dam_type"] == "Observation":
                    entry = DataEntryMetadataEntry(object_id=obj_md["metadata"]["subject"], metadata_schema_id=schema.id, id=md_id)
                elif subject["dam_type"] == "Dataset":
                    entry = DatasetMetadataEntry(object_id=obj_md["metadata"]["subject"], metadata_schema_id=schema.id, id=md_id)
                else:
                    raise UnknownObjectError("Not a valid metadata object to retrieve")
                    
                self._copy_attrs(obj_md["data"], entry)
                return entry
        except dam.DAMException as e:
            logger.exception("Exception while getting object metadata")
            raise PersistenceError("Error getting object metadata: %s"%(str(e))) 
    
