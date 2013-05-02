"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.utils import format_timestamp, parse_timestamp
from dc24_ingester_platform.service import BaseRepositoryService
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
from jcudc24ingesterapi.models.metadata import DatasetMetadataEntry, DataEntryMetadataEntry
from jcudc24ingesterapi.schemas import ConcreteSchema

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
        

class RepositoryDB(BaseRepositoryService):
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
    
    def find_data_entries(dataset_id):
        """Find all observations within this dataset"""
        s = orm.sessionmaker(bind=self.engine)()
        ret = []
        try:
            objs = s.query(Observation).filter(Observation.dataset == dataset_id).all()
            for obj in objs:
                pass
        finally:
            s.close()

    def persist_data_entry(self, dataset, schema, data_entry, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(data_entry.data, schema.attrs)
        
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obs = Observation()
            obs.timestamp = data_entry.timestamp
            obs.dataset = dataset.id
            
            session.add(obs)
            session.flush()
            
            # Copy all files into place
            self.copy_files(data_entry.data, schema.attrs, cwd, obs, "data_entry")
            
            merge_parameters(obs.attrs, data_entry.data, ObservationAttr)
            session.merge(obs)
            session.flush()
            session.commit()
            
            return self._get_data_entry(obs.dataset, obs.id, session)
        finally:
            session.close()
            
    def get_data_entry(self, dataset_id, data_entry_id):
        
        session = orm.sessionmaker(bind=self.engine)()
        try:
            return self._get_data_entry(dataset_id, data_entry_id, session)
        finally:
            session.close()
            
    def get_data_entry_stream(self, dataset_id, data_entry_id, attr):
        """Get a file stream for the data entry"""
        data_entry = self.get_data_entry(dataset_id, data_entry_id)
        return open(data_entry[attr].f_path, "rb")
    
    def _get_data_entry(self, dataset_id, data_entry_id, session):
        obs = session.query(Observation).filter(Observation.dataset == dataset_id,
                                                Observation.id == data_entry_id).one()
        dataset = self.service.get_dataset(obs.dataset)
        schema = ConcreteSchema(self.service.get_schema_tree(dataset.schema))
        
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
            
    def persist_dataset_metadata(self, dataset, schema, attrs, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            md = DatasetMetadata()
            md.dataset = dataset.id
            md.schema = schema.id
            
            s.add(md)
            s.flush()
            
            # Copy all files into place
            self.copy_files(attrs, schema.attrs, cwd, md, "dataset_metadata")
            
            merge_parameters(md.attrs, attrs, DatasetMetadataAttr)
            s.merge(md)
            s.flush()
            s.commit()
            
            entry = DatasetMetadataEntry(object_id=md.dataset, metadata_schema_id=md.schema, id=md.id)
            for attr in md.attrs:
                if isinstance(schema.attrs[attr.name], FileDataType):
                    entry[attr.name] = FileObject(f_path=attr.value) 
                else:
                    entry[attr.name] = attr.value
            return entry 
        finally:
            s.close()
            
    def persist_data_entry_metadata(self, data_entry, schema, attrs, cwd):
        # Check the attributes are actually in the schema
        self.validate_schema(attrs, schema.attrs)
        s = orm.sessionmaker(bind=self.engine)()
        try:
            md = DataEntryMetadata()
            md.data_entry = data_entry.id
            md.schema = schema.id
            
            s.add(md)
            s.flush()
            
            # Copy all files into place
            self.copy_files(attrs, schema.attrs, cwd, md, "data_entry_metadata")
            
            merge_parameters(md.attrs, attrs, DataEntryMetadataAttr)
            s.merge(md)
            s.flush()
            s.commit()
            
            entry = DataEntryMetadataEntry(object_id=md.dataset, metadata_schema_id=md.schema, id=md.id)
            for attr in md.attrs:
                if isinstance(schema.attrs[attr.name], FileDataType):
                    entry[attr.name] = FileObject(f_path=attr.value) 
                else:
                    entry[attr.name] = attr.value
            return entry 
                    
        finally:
            s.close()
