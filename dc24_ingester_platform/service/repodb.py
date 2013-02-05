"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.utils import format_timestamp, parse_timestamp
from dc24_ingester_platform.service import IRepositoryService
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, ForeignKey, DateTime
import sqlalchemy.orm as orm
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound
import decimal
import logging
import os
import shutil
from jcudc24ingesterapi.schemas.data_types import FileDataType
from jcudc24ingesterapi.models.data_entry import DataEntry, FileObject

logger = logging.getLogger(__name__)

Base = declarative_base()

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

class DatasetMetadata(Base):
    __tablename__ = "DATASET_METADATA"
    id = Column(Integer, primary_key=True)
    dataset = Column(Integer)
    schema = Column(Integer)
    attrs = orm.relationship("DatasetMetadataAttr")

class DatasetMetadataAttr(Base):
    __tablename__ = "DATASET_METADATA_ATTRS"
    id = Column(Integer, primary_key=True)
    metadata_entry = Column(Integer, ForeignKey('DATASET_METADATA.id'))
    name = Column(String)
    value = Column(String)
    
class Observation(Base):
    __tablename__ = "OBSERVATIONS"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    dataset = Column(Integer)
    attrs = orm.relationship("ObservationAttr")

class ObservationAttr(Base):
    __tablename__ = "OBSERVATION_ATTRS"
    id = Column(Integer, primary_key=True)
    observation = Column(Integer, ForeignKey('OBSERVATIONS.id'))
    name = Column(String)
    value = Column(String)

class DataEntryMetadata(Base):
    __tablename__ = "DATA_ENTRY_METADATA"
    id = Column(Integer, primary_key=True)
    data_entry = Column(Integer)
    schema = Column(Integer)
    attrs = orm.relationship("DataEntryMetadataAttr")

class DataEntryMetadataAttr(Base):
    __tablename__ = "DATA_ENTRY_METADATA_ATTRS"
    id = Column(Integer, primary_key=True)
    metadata_entry = Column(Integer, ForeignKey('DATA_ENTRY_METADATA.id'))
    name = Column(String)
    value = Column(String)
    
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
            setattr(obj, value_attr, working[obj.name].f_path if isinstance(working[obj.name], FileObject) else working[obj.name])
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
        setattr(obj, value_attr, working[obj.name].f_path if isinstance(working[obj.name], FileObject) else working[obj.name])
        col_orig.append(obj)
        

class RepositoryDB(IRepositoryService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, config):
        self.engine = create_engine(config["db"])
        self.repo = config["files"]
        
        if not os.path.exists(self.repo):
            os.makedirs(self.repo)
        
        Observation.metadata.create_all(self.engine, checkfirst=True)
    
    def reset(self):
        Observation.metadata.drop_all(self.engine, checkfirst=True)
        Observation.metadata.create_all(self.engine, checkfirst=True)
    
    def copy_files(self, attrs, schema, cwd, obj, obj_type):
        """Copy file attributes into place and update the File Objects
        to point to the destination path."""
        obj_path = os.path.join(self.repo, obj_type)
        if not os.path.exists(obj_path): os.makedirs(obj_path)
        for k in attrs:
            if isinstance(schema[k], FileDataType):
                dest_file_name = os.path.join(obj_path, "%d-%s"%(obj.id, k))
                shutil.copyfile(os.path.join(cwd, attrs[k].f_path), dest_file_name)
                attrs[k].f_path = dest_file_name
    
    def validate_schema(self, attrs, schema):
        """Validate the attributes against the schema"""
        for k in attrs:
            if k not in schema:
                raise ValueError("%s is not in the schema"%(k))

    def findObservations(dataset_id):
        """Find all observations within this dataset"""
        s = orm.sessionmaker(bind=self.engine)()
        ret = []
        try:
            objs = s.query(IngesterLog).filter(Observation.dataset == dataset_id).all()
            for obj in objs:
                pass
        finally:
            s.close()

    def persistObservation(self, dataset, schema, timestamp, attrs, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            obs = Observation()
            obs.timestamp = timestamp
            obs.dataset = dataset.id
            
            s.add(obs)
            s.flush()
            
            # Copy all files into place
            self.copy_files(attrs, schema.attrs, cwd, obs, "data_entry")
            
            merge_parameters(obs.attrs, attrs, ObservationAttr)
            s.merge(obs)
            s.flush()
            s.commit()
            
            entry = DataEntry()
            entry.dataset = obs.dataset
            entry.id = obs.id
            entry.timestamp = obs.timestamp
            for attr in obs.attrs:
                if isinstance(schema.attrs[attr.name], FileDataType):
                    entry[attr.name] = FileObject(f_path=attr.value) 
                else:
                    entry[attr.name] = attr.value 
            return entry
        finally:
            s.close()
            
    def persistDatasetMetadata(self, dataset, schema, attrs, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            obs = DatasetMetadata()
            obs.dataset = dataset.id
            obs.schema = schema.id
            
            s.add(obs)
            s.flush()
            
            # Copy all files into place
            self.copy_files(attrs, schema.attrs, cwd, obs, "dataset_metadata")
            
            merge_parameters(obs.attrs, attrs, DatasetMetadataAttr)
            s.merge(obs)
            s.flush()
            s.commit()
            
            return {"class":"dataset_metadata_entry", "object_id":obs.dataset, "metadata_schema":schema["id"], "id":obs.id}
        finally:
            s.close()
            
    def persistDataEntryMetadata(self, data_entry, schema, attrs, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            obs = DataEntryMetadata()
            obs.data_entry = data_entry.id
            obs.schema = schema.id
            
            s.add(obs)
            s.flush()
            
            # Copy all files into place
            self.copy_files(attrs, schema.attrs, cwd, obs, "data_entry_metadata")
            
            merge_parameters(obs.attrs, attrs, DataEntryMetadataAttr)
            s.merge(obs)
            s.flush()
            s.commit()
            
            return {"class":"data_entry_metadata_entry", "object_id":obs.dataset, "metadata_schema":schema["id"], "id":obs.id}
        finally:
            s.close()
