"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey
import sqlalchemy.orm as orm
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound
import decimal
import logging

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

class Location(Base):
    __tablename__ = "LOCATIONS"
    __xmlrpc_class__ = "location"
    id = Column(Integer, primary_key=True)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    name = Column(String)
    elevation = Column(DECIMAL)

class Dataset(Base):
    __tablename__ = "DATASETS"
    __xmlrpc_class__ = "dataset"
    id = Column(Integer, primary_key=True)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    location = Column(Integer, ForeignKey('LOCATIONS.id'))
    data_source = orm.relationship("DataSource", uselist=False)
    sampling = orm.relationship("Sampling", uselist=False)
    schema = orm.relationship("SchemaEntry")

class Sampling(Base):
    """A DataSource is a generic data storage class"""
    __tablename__ = "SAMPLING"
    __xmlrpc_class__ = "sampling"
    id = Column(Integer, primary_key=True)
    kind = Column(String)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
    parameters = orm.relationship("SamplingParameter")

class SamplingParameter(Base):
    __tablename__ = "SAMPLING_PARAMETERS"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    sampling_id = Column(Integer, ForeignKey("SAMPLING.id"))
    
class DataSource(Base):
    """A DataSource is a generic data storage class"""
    __tablename__ = "DATA_SOURCES"
    __xmlrpc_class__ = "data_source"
    id = Column(Integer, primary_key=True)
    kind = Column(String)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
    parameters = orm.relationship("DataSourceParameter")

class DataSourceParameter(Base):
    __tablename__ = "DATA_SOURCE_PARAMETERS"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    dataset_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))
    
class SchemaEntry(Base):
    __tablename__ = "SCHEMA"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    kind = Column(String)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))

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
        

class IngesterServiceDB(IIngesterService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Location.metadata.create_all(self.engine, checkfirst=True)
        Dataset.metadata.create_all(self.engine, checkfirst=True)
#        Sampling.metadata.create_all(self.engine, checkfirst=True)
#        SamplingParameter.metadata.create_all(self.engine, checkfirst=True)
#        DataSource.metadata.create_all(self.engine, checkfirst=True)
#        DataSourceParameter.metadata.create_all(self.engine, checkfirst=True)
    
    def persistDataset(self, dataset):
        s = orm.sessionmaker(bind=self.engine)()
        dataset = dataset.copy() # Make a copy so we can remove keys we don't want
        
        ds = Dataset()
        schema = dataset["schema"]
        data_source = dataset["data_source"].copy() if dataset.has_key("data_source") and dataset["data_source"] != None else None
        sampling = dataset["sampling"].copy() if dataset.has_key("sampling") and dataset["sampling"] != None else None
        if dataset.has_key("data_source"): del dataset["data_source"]
        if dataset.has_key("sampling"): del dataset["sampling"]
        if dataset.has_key("schema"): del dataset["schema"]
        
        try:
            if dataset.has_key("id") and dataset["id"] != None:
                ds = obj_to_dict(s.query(Dataset).filter(Dataset.id == dataset["id"]).one())
            dict_to_object(dataset, ds)
            # Clean up the sampling link
            if ds.data_source == None and data_source != None:
                ds.data_source = DataSource()
            elif ds.data_source != None and data_source == None:
                del ds.data_source
            # If the sampling object actually exists then populate it
            if ds.data_source != None:
                ds.data_source.kind = data_source["class"]
                del data_source["class"]
                merge_parameters(ds.data_source.parameters, data_source, DataSourceParameter)
            
            # Clean up the sampling link
            if ds.sampling == None and sampling != None:
                ds.sampling = Sampling()
            elif ds.sampling != None and sampling == None:
                del ds.sampling
            # If the sampling object actually exists then populate it
            if ds.sampling != None:
                ds.sampling.kind = sampling["class"]
                del sampling["class"]
                merge_parameters(ds.sampling.parameters, sampling, SamplingParameter)
            
            # If the sampling object actually exists then populate it
            if ds.schema != None:
                merge_parameters(ds.schema, schema, SchemaEntry, value_attr="kind")
            
            self.persist(s, ds)
            return self.getDataset(ds.id)
        finally:
            s.close()
        
    def persistLocation(self, dataset):
        s = orm.sessionmaker(bind=self.engine)()
        try:
            loc = Location()
            dict_to_object(dataset, loc)
            return self.persist(s, loc)
        finally:
            s.close()
        
    def persist(self, s, obj):
        """Persists the object using the provided session. Will rollback
        but will not close the session
        """
        try:
            if obj.id == None:
                s.add(obj)
            else:
                s.merge(obj)
            s.flush()
            s.commit()
            return obj_to_dict(obj)
        except Exception, e:
            logger.error("Error saving: " + str(e))
            s.rollback()
            raise Exception("Could not save dataset")
            
    def deleteDataset(self, dataset):
        pass
    
    def getDataset(self, id=None):
        """Get the dataset as a DTO"""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            obj = s.query(Dataset).filter(Dataset.id == id).one()
            ret = obj_to_dict(obj)
            # Retrieve data_source
            if obj.data_source != None:
                data_source = {}
                data_source["class"] = str(obj.data_source.kind)
                for entry in obj.data_source.parameters:
                    data_source[str(entry.name)] = str(entry.value)
                ret["data_source"] = data_source
            # Retrieve sampling
            if obj.sampling != None:
                sampling = {}
                sampling["class"] = str(obj.sampling.kind)
                for entry in obj.sampling.parameters:
                    sampling[str(entry.name)] = str(entry.value)
                ret["sampling"] = sampling
            ret["schema"] = {}
            for entry in obj.schema:
                ret["schema"][str(entry.name)] = str(entry.kind)
            return ret
        except NoResultFound, e:
            return None
        finally:
            s.close()
        
