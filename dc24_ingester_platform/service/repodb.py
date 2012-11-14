"""
Created on Oct 5, 2012

@author: nigel
"""
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
    
    def persistObservation(self, dataset, schema, timestamp, attrs, cwd):
        schema = schema["attributes"]
        # Check the attributes are actually in the schema
        for k in attrs:
            if k not in schema:
                raise ValueError("%s is not in the schema"%(k))
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            obs = Observation()
            obs.timestamp = timestamp
            obs.dataset = dataset["id"]
            
            s.add(obs)
            s.flush()
            
            # Copy all files into place
            for k in attrs:
                if schema[k] == "file":
                    dest_file_name = os.path.join(self.repo, "%d-%s"%(obs.id, k))
                    shutil.copyfile(os.path.join(cwd, attrs[k]), dest_file_name)
                    attrs[k] = dest_file_name
            merge_parameters(obs.attrs, attrs, ObservationAttr)
            s.merge(obs)
            s.flush()
            s.commit()
            
        finally:
            s.close()
