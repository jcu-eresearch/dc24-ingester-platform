"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, ForeignKey, DateTime
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
        if type(getattr(obj, attr)) in (str, int, float, unicode, dict, bool, type(None)):
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
    location = Column(Integer, ForeignKey('LOCATIONS.id'))
    data_source = orm.relationship("DataSource", uselist=False)
    sampling = orm.relationship("Sampling", uselist=False)
    schema = Column(Integer, ForeignKey('SCHEMA.id'))
    enabled = Column(Boolean, default=True)

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

class Schema(Base):
    __tablename__ = "SCHEMA"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    for_ = Column(String, name="for")
    attributes = orm.relationship("SchemaAttribute")
    
class SchemaAttribute(Base):
    __tablename__ = "SCHEMA_ATTRIBUTE"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    kind = Column(String)
    schema_id = Column(Integer, ForeignKey("SCHEMA.id"))

class IngesterLog(Base):
    __tablename__ = "INGESTER_LOG"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    level = Column(String)
    message = Column(String)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))

class SamplerState(Base):
    __tablename__ = "SAMPLER_STATE"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    dataset_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))
 
class DataSourceState(Base):
    __tablename__ = "DATA_SOURCE_STATE"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    dataset_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))   
    
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
        
def method(verb, cls):
    """Annotation for identifying which class methods are responsible
    for different actions and classes
    :param verb: Action (persist, get, delete)
    :param cls: The serialised class name string
    """ 
    def _method(fn):
        fn.verb = verb
        fn.cls = cls
        return fn
    return _method

class IngesterServiceDB(IIngesterService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Location.metadata.create_all(self.engine, checkfirst=True)
        
        self.samplers = {}
        self.data_source = {}

    def reset(self):
        Location.metadata.drop_all(self.engine)
        Location.metadata.create_all(self.engine, checkfirst=True)

    def commit(self, unit):
        s = orm.sessionmaker(bind=self.engine)()
        ret = []
        locs = {}
        datasets = {}
        try:
            # delete first
            # now sort to find objects by order of dependency (location then dataset)
            for obj in [o for o in unit["insert"] if o["class"] == "location"]:
                id = obj["id"]
                del obj["id"]
                obj = self.persistLocation(obj, s)
                locs[id] = obj["id"]
                obj["correlationid"] = id
                ret.append(obj)
    
            for obj in [o for o in unit["insert"] if o["class"] == "dataset"]:
                id = obj["id"]
                del obj["id"]
                if obj["location"] < 0: obj["location"] = locs[obj["location"]]
                obj = self.persistDataset(obj, s)
                datasets[id] = obj["id"]
                obj["correlationid"] = id
                ret.append(obj)
            s.commit()
            return ret
        finally:
            s.close()

    def persist(self, obj):
        cls = obj["class"]
        del obj["class"]
        for fn in dir(self):
            fn = getattr(self, fn)
            if hasattr(fn, "verb") and hasattr(fn, "cls") and fn.verb == "persist" and fn.cls == cls:
                s = orm.sessionmaker(bind=self.engine)()
                try:
                    obj = fn(obj, s)
                    s.commit()
                    return obj
                finally:
                    s.close()
        raise ValueError("%s not supported"%(cls))

    @method("persist", "dataset")
    def persistDataset(self, dataset, s):
        dataset = dataset.copy() # Make a copy so we can remove keys we don't want
        
        ds = Dataset()
        schema = dataset["schema"]
        data_source = dataset["data_source"].copy() if dataset.has_key("data_source") and dataset["data_source"] != None else None
        sampling = dataset["sampling"].copy() if dataset.has_key("sampling") and dataset["sampling"] != None else None
        if dataset.has_key("data_source"): del dataset["data_source"]
        if dataset.has_key("sampling"): del dataset["sampling"]
        if dataset.has_key("schema"): del dataset["schema"]
        
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
        
        self._persist(ds, s)
        return self._getDataset(ds.id, s)
    
    @method("persist", "location")    
    def persistLocation(self, location, s):
        loc = Location()
        dict_to_object(location, loc)
        return self._persist(loc, s)
    
    @method("persist", "dataset_metadata_schema")    
    def persistDatasetMetaDataSchema(self, schema, s):
        return self._persistSchema(schema, "dataset_metadata", s)

    @method("persist", "data_entry_schema")    
    def persistDataEntrySchema(self, schema, s):
        return self._persistSchema(schema, "data_entry", s)
        
    def _persistSchema(self, schema, for_, s):
        schema = schema.copy()
        attrs = schema["attributes"]
        del schema["attributes"]
        schema_ = Schema()
        dict_to_object(schema, schema_)
        merge_parameters(schema_.attributes, attrs, SchemaAttribute, value_attr="kind")
        
        schema_.for_ = for_
        return self._persist(schema_, s)
        
    def _persist(self, obj, session):
        """Persists the object using the provided session. Will rollback
        but will not close the session
        """
        try:
            if obj.id == None:
                session.add(obj)
            else:
                session.merge(obj)
            session.flush()
            return obj_to_dict(obj)
        except Exception, e:
            logger.error("Error saving: " + str(e))
            session.rollback()
            raise Exception("Could not save dataset")
            
    def deleteDataset(self, dataset):
        pass
    
    def getDataset(self, ds_id):
        """Get the dataset as a DTO"""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            return self._getDataset(ds_id, s)
        finally:
            s.close()
        
    def _getDataset(self, ds_id, session):
        """Private method to actually get the dataset using the session provided.
        """
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
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
            return ret
        except NoResultFound, e:
            return None
        
    def enableDataset(self, ds_id):
        """Enable the dataset"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            obj.enabled = True
            session.merge(obj)
            session.commit()
        finally:
            session.close()
    
    def disableDataset(self, ds_id):
        """Disable the dataset"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            obj.enabled = False
            session.merge(obj)
            session.commit()
        finally:
            session.close()
        
    def getActiveDatasets(self):
        """Returns all enabled datasets."""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            objs = s.query(Dataset).filter(Dataset.enabled == True).all()
            ret_list = []
            for obj in objs:
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
                ret_list.append(ret)
            return ret_list
        except NoResultFound, e:
            return None
        finally:
            s.close()

    def logIngesterEvent(self, dataset_id, timestamp, level, message):
        s = orm.sessionmaker(bind=self.engine)()
        try:
            log = IngesterLog()
            log.dataset_id = dataset_id
            log.timestamp = timestamp
            log.level = level
            log.message = message
            s.add(log)
            s.flush()
            s.commit()
        finally:
            s.close()
    
    def getIngesterEvents(self, dataset_id):
        s = orm.sessionmaker(bind=self.engine)()
        try:
            objs = s.query(IngesterLog).filter(IngesterLog.dataset_id == dataset_id).all()
            ret_list = []
            for obj in objs:
                ret_list.append(obj_to_dict(obj))
            return ret_list
        finally:
            s.close()
            
    def persistSamplerState(self, id, state):
        self.samplers[id] = state
    
    def getSamplerState(self, id):
        if id not in self.samplers: return {}
        return self.samplers[id]

    def persistDataSourceState(self, id, state):
        self.data_source[id] = state
    
    def getDataSourceState(self, id):
        if id not in self.data_source: return {}
        return self.data_source[id]
