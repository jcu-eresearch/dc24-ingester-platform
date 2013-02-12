"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService, find_method, method, PersistenceError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, ForeignKey, DateTime
import sqlalchemy.orm as orm
from sqlalchemy.schema import Table
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound
import decimal
import logging
from dc24_ingester_platform.utils import parse_timestamp, format_timestamp

import jcudc24ingesterapi.models.data_entry
import jcudc24ingesterapi.models.locations
import jcudc24ingesterapi.models.dataset
import jcudc24ingesterapi.models.data_sources
import jcudc24ingesterapi.schemas.data_entry_schemas
import jcudc24ingesterapi.schemas.data_types
from jcudc24ingesterapi.models.locations import LocationOffset
from jcudc24ingesterapi.ingester_platform_api import get_properties, Marshaller
import datetime

logger = logging.getLogger(__name__)

Base = declarative_base()

domain_marshaller = Marshaller()

def obj_to_dict(obj, klass=None):
    """Maps an object of base class BaseManagementObject to a dict.
    """
    ret = {}
    for attr in dir(obj):
        if attr.startswith("_") or attr == "metadata": continue
        v = getattr(obj, attr)
        if type(v) in (str, int, float, unicode, dict, bool, type(None)):
            ret[attr] = v
        elif type(v) == decimal.Decimal:
            ret[attr] = float(v)
        elif type(v) == datetime.datetime:
            ret[attr] = format_timestamp(v)
    if klass != None: ret["class"] = klass
    elif hasattr(obj, "__xmlrpc_class__"): ret["class"] = obj.__xmlrpc_class__

    if ret["class"] == "schema":
        ret["class"] = ret["for_"] + "_schema"
        del ret["for_"]
        ret["attributes"] = [{"name":attr.name, "class":attr.kind, "description":attr.description, "units":attr.units} for attr in obj.attributes]
        ret["extends"] = [obj_to_dict(p) for p in obj.extends]
    elif ret["class"] == "region":
        obj.region_points.sort(cmp=lambda a,b: cmp(a.order,b.order))
        ret["region_points"] = [(point.latitude, point.longitude) for point in obj.region_points]
    elif ret["class"] == "dataset":
        if ret["x"] != None:
            ret["location_offset"] = {"class":"offset", "x":ret["x"], 
                                      "y":ret["y"], "z":ret["z"]}
        del ret["x"]
        del ret["y"]
        del ret["z"]
    return ret

#def dict_to_object(dic, obj):
#    for attr in dir(obj):
#        if attr.startswith("_"): continue
#        if dic.has_key(attr): setattr(obj, attr, dic[attr])

class Region(Base):
    __tablename__ = "REGION"
    __xmlrpc_class__ = "region"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    #parentRegions = orm.relationship("Region")
    region_points = orm.relationship("RegionPoint")
    
class RegionPoint(Base):
    __tablename__ = "REGION_POINT"
    id = Column(Integer, primary_key=True)
    order = Column(Integer, unique=True)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    region_id = Column(Integer, ForeignKey("REGION.id"))
    
    def __init__(self, lat=None, lng=None, order=None):
        self.latitude = lat
        self.longitude = lng
        self.order = order

class Location(Base):
    __tablename__ = "LOCATIONS"
    __xmlrpc_class__ = "location"
    id = Column(Integer, primary_key=True)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    name = Column(String)
    elevation = Column(DECIMAL)
    repository_id = Column(String)
    #region = orm.relationship("Region", uselist=False)

class Dataset(Base):
    __tablename__ = "DATASETS"
    __xmlrpc_class__ = "dataset"
    id = Column(Integer, primary_key=True)
    location = Column(Integer, ForeignKey('LOCATIONS.id'))
    data_source = orm.relationship("DataSource", uselist=False)
    schema = Column(Integer, ForeignKey('SCHEMA.id'))
    enabled = Column(Boolean, default=True)
    description = Column(String)
    redbox_uri = Column(String)
    repository_id = Column(String)
    # FIXME: Move to separate schema
    x = Column(DECIMAL)
    y = Column(DECIMAL)
    z = Column(DECIMAL)

class Sampling(Base):
    """A DataSource is a generic data storage class"""
    __tablename__ = "SAMPLING"
    __xmlrpc_class__ = "sampling"
    id = Column(Integer, primary_key=True)
    kind = Column(String)
    data_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))
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
    sampling = orm.relationship("Sampling", uselist=False)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
    parameters = orm.relationship("DataSourceParameter")
    processing_script = Column(String(32000))

class DataSourceParameter(Base):
    __tablename__ = "DATA_SOURCE_PARAMETERS"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    dataset_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))

schema_to_schema = Table("schema_to_schema", Base.metadata,
    Column("child_id", Integer, ForeignKey("SCHEMA.id"), primary_key=True),
    Column("parent_id", Integer, ForeignKey("SCHEMA.id"), primary_key=True)
)

class Schema(Base):
    __tablename__ = "SCHEMA"
    __xmlrpc_class__ = "schema"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    for_ = Column(String, name="for")
    attributes = orm.relationship("SchemaAttribute")
    repository_id = Column(String)
    extends = relationship("Schema",
        secondary=schema_to_schema,
        primaryjoin=id==schema_to_schema.c.child_id,
        secondaryjoin=id==schema_to_schema.c.parent_id)
    
class SchemaAttribute(Base):
    __tablename__ = "SCHEMA_ATTRIBUTE"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    kind = Column(String)
    units = Column(String)
    description = Column(String)
    schema_id = Column(Integer, ForeignKey("SCHEMA.id"))

class IngesterLog(Base):
    __tablename__ = "INGESTER_LOG"
    __xmlrpc_class__ = "ingester_log"
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
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
 
class DataSourceState(Base):
    __tablename__ = "DATA_SOURCE_STATE"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))   
    
def dict_to_obj(data):
    """Copies a dict onto an object"""
    obj = object()
    for k in data:
        setattr(obj, k, data[k])
    return obj

def merge_parameters(src, dst, klass, name_attr="name", value_attr="value", ignore_props=[]):
    """This method updates col_orig removing any that aren't in col_new, updating those that are, and adding new ones
    using klass as the constructor
    
    :param src: is an object
    :param dst: is a list of klass objects
    :param klass: is a type
    """
    to_del = []
    props = get_properties(src)
    for ig in ignore_props: 
        if ig in props: props.remove(ig)
    
    for obj in dst:
        prop_name = getattr(obj,name_attr)
        if prop_name in props:
            # Update
            setattr(obj, value_attr, getattr(src, prop_name))
            props.remove(prop_name)
        else:
            # Delete pending
            to_del.append(obj)
    # Delete
    for obj in to_del:
        dst.remove(obj)
    # Add
    for k in props:
        obj = klass()
        setattr(obj, name_attr, k)
        setattr(obj, value_attr, getattr(src, k))
        dst.append(obj)
        
def merge_schema_lists(col_orig, col_new):
    """This method updates col_orig removing any that aren't in col_new, updating those that are, and adding new ones
    using klass as the constructor
    
    col_new is a list
    col_orig is a list
    """
    working = dict([(obj.name, obj) for obj in col_new])
    to_del = []
    for obj in col_orig:
        if obj.name in working:
            # Update
            obj.description = working[obj.name].description
            del working[obj.name]
        else:
            # Delete pending
            to_del.append(obj)
    # Delete
    for obj in to_del:
        col_orig.remove(obj)
    # Add
    for k in working:
        col_orig.append(working[k])

def parameters_to_dict(params, name_attr="name", value_attr="value"):
    """Map a parameters set back to a dict"""
    ret = {}
    for obj in params:
        k = getattr(obj, name_attr)
        v = getattr(obj, value_attr)
        ret[k] = v
    return ret
        
def ingest_order(x, y):
    """Sort objects by class according to the order which will make an insert transaction work.
    """
    order = ["_schema", "region", "location", "dataset"]
    x_i = len(order)
    y_i = len(order)
    for i in range(len(order)): 
        if x.__xmlrpc_class__.endswith(order[i]): 
            x_i = i
            break
    for i in range(len(order)): 
        if y.__xmlrpc_class__.endswith(order[i]): 
            y_i = i
            break
    return cmp(x_i, y_i)

def copy_attrs(src, dst, attrs):
    """Copy a list of attributes from one object to another"""
    for attr in attrs:
        if hasattr(src, attr):
            v = getattr(src, attr)
            if isinstance(v, Base):
                setattr(dst, attr, dao_to_domain(v))
            else:
                setattr(dst, attr, v)
                
def copy_parameters(src, dst, attrs, name_attr="name", value_attr="value"):
    """Copy from a property list to an object"""
    for attr in src:
        k = getattr(attr, name_attr)
        v = getattr(attr, value_attr)
        if k in attrs:
            setattr(dst, k, v)

def dao_to_domain(dao):
    """Copies a DAO object to a domain object"""
    domain = None
    if type(dao) == Region:
        domain = jcudc24ingesterapi.models.locations.Region()
        copy_attrs(dao, domain, ["id", "name"])
        domain.region_points = [(p.latitude, p.longitude) for p in dao.region_points]
    elif type(dao) == Schema:
        domain = domain_marshaller.class_for(dao.for_ + "_schema")()
        
        copy_attrs(dao, domain, ["id", "name", "repository_id"])
        domain.extends.extend(dao.extends)
        for attr in dao.attributes:
            attr_ = None
            if attr.kind == "file": attr_ = jcudc24ingesterapi.schemas.data_types.FileDataType(attr.name)
            elif attr.kind == "string": attr_ = jcudc24ingesterapi.schemas.data_types.String(attr.name)
            elif attr.kind == "integer": attr_ = jcudc24ingesterapi.schemas.data_types.Integer(attr.name)
            elif attr.kind == "double": attr_ = jcudc24ingesterapi.schemas.data_types.Double(attr.name)
            elif attr.kind == "datetime": attr_ = jcudc24ingesterapi.schemas.data_types.DateTime(attr.name)
            elif attr.kind == "boolean": attr_ = jcudc24ingesterapi.schemas.data_types.Boolean(attr.name)
            else: raise PersistenceError("Invalid data type: %s"%attr.kind)
            attr_.units = attr.units
            attr_.description = attr.description
            domain.addAttr(attr_)
    elif type(dao) == Location:
        domain = jcudc24ingesterapi.models.locations.Location()
        copy_attrs(dao, domain, ["id", "name", "latitude", "longitude", "elevation", "repository_id"])
    elif type(dao) == Dataset:
        domain = jcudc24ingesterapi.models.dataset.Dataset()
        copy_attrs(dao, domain, ["id", "location", "schema", "enabled", "description", "redbox_uri", "repository_id"])
        if dao.x != None:
            domain.location_offset = LocationOffset(dao.x, dao.y, dao.z)
        if dao.data_source != None:
            domain.data_source = domain_marshaller.class_for(dao.data_source.kind)()
            copy_attrs(dao.data_source, domain.data_source, get_properties(domain.data_source))
            copy_parameters(dao.data_source.parameters, domain.data_source, get_properties(domain.data_source))
    elif type(dao) == Sampling:
        domain = domain_marshaller.class_for(dao.kind)()
        copy_attrs(dao, domain, get_properties(domain))
        copy_parameters(dao.parameters, domain, get_properties(domain))
    else:
        raise PersistenceError("Could not convert DAO object to domain: %s"%(str(type(dao))))
    return domain

class IngesterServiceDB(IIngesterService):
    """This service provides DAO operations for the ingester service.
    
    All objects/DTOs passed in and out of this service are dicts. This service protects the storage layer.
    """
    def __init__(self, db_url, repo):
        self.engine = create_engine(db_url)
        Location.metadata.create_all(self.engine, checkfirst=True)
        
        self.samplers = {}
        self.data_source = {}
        self.repo = repo
        # Give the repo a reference to this service.
        self.repo.service = self 
        self.obs_listeners = []

    def register_observation_listener(self, listener):
        self.obs_listeners.append(listener)
        
    def unregister_observation_listener(self, listener):
        self.obs_listeners.remove(listener)

    def reset(self):
        Location.metadata.drop_all(self.engine)
        Location.metadata.create_all(self.engine, checkfirst=True)
        self.repo.reset()

    def commit(self, unit, cwd):
        """Commit a unit of work with file objects based in cwd.
        
        This method will alter the unit and it's contents, and returns 
        a list of the new persisted objects.
        """
        s = orm.sessionmaker(bind=self.engine)()
        ret = []
        locs = {}
        schemas = {}
        datasets = {}
        try:
            unit._to_insert.sort(ingest_order)
            unit._to_update.sort(ingest_order)
            # delete first
            # now sort to find objects by order of dependency (location then dataset)
            for unit_list in (unit._to_insert, unit._to_update):
                for obj in unit_list:
                    oid = obj.id
                    if obj.id < 0: obj.id = None
                    cls = obj.__xmlrpc_class__
                    if cls == "dataset":
                        if obj.location < 0: obj.location = locs[obj.location]
                        if obj.schema < 0: obj.schema = schemas[obj.schema]
                        
                    elif cls.endswith("schema"):
                        obj.extends = [ schemas[p_id] if p_id<0 else p_id for p_id in obj.extends]
                            
                    fn = find_method(self, "persist", cls)
                    if fn == None:
                        raise ValueError("Could not find method for", "persist", cls)
                    obj = fn(obj, s, cwd)
                    if cls == "location":
                        locs[oid] = obj.id
                    elif cls.endswith("schema"):
                        schemas[oid] = obj.id
                            
                    obj.correlationid = oid
                    ret.append(obj)
            for obj_id in unit._to_enable:
                self.enableDataset(obj_id)
            for obj_id in unit._to_disable:
                self.disableDataset(obj_id)
            s.commit()
            return ret
        finally:
            s.close()

    def persist(self, obj, cwd=None):
        """The main entry point for persistence. Handles sessions."""
        cls = obj.__xmlrpc_class__
        fn = find_method(self, "persist", cls)
        if fn != None:
            s = orm.sessionmaker(bind=self.engine)()
            try:
                obj = fn(obj, s, cwd)
                s.commit()
                return obj
            finally:
                s.close()
        raise ValueError("%s not supported"%(cls))

    @method("persist", "dataset")
    def persistDataset(self, dataset, session, cwd):
        """Assumes that we have a copy of the object, so we can change it if required.
        """
        if dataset.location == None:
            raise ValueError("Location must be set")
        # Check schema is of the correct type

        try:
            location = session.query(Location).filter(Location.id == dataset.location).one()
        except NoResultFound, e:
            raise ValueError("Provided location not found: %d"%dataset.location)
        try:
            schema = session.query(Schema).filter(Schema.id == dataset.schema).one()
        except NoResultFound, e:
            raise ValueError("Provided schema not found: %d"%dataset.schema)
        if schema.for_ != "data_entry":
            raise ValueError("The schema must be for a data_entry")
        
        ds = Dataset()
        if dataset.id != None:
            ds = session.query(Dataset).filter(Dataset.id == dataset.id).one()
            
        copy_attrs(dataset, ds, ["location", "schema", "enabled", "description", "redbox_uri", "sampling_script"])
        if dataset.location_offset != None:
            try:
                ds.x = dataset.location_offset.x
                ds.y = dataset.location_offset.y
                ds.z = dataset.location_offset.z
            except:
                raise ValueError("Location offset is invalid")

#        # Clean up the data source link
        if ds.data_source == None and dataset.data_source != None:
            ds.data_source = DataSource()
        elif ds.data_source != None and dataset.data_source == None:
            del ds.data_source
        # If the data source object actually exists then populate it
        if ds.data_source != None:
            ds.data_source.kind = dataset.data_source.__xmlrpc_class__
            merge_parameters(dataset.data_source, ds.data_source.parameters, DataSourceParameter, ignore_props=["sampling"])
            ds.data_source.processing_script = dataset.data_source.processing_script
        
        if ds.data_source != None and hasattr(dataset.data_source, "sampling"):
            # Figure out sampling now
            if ds.data_source.sampling == None and dataset.data_source.sampling != None:
                ds.data_source.sampling = Sampling()
            elif ds.data_source.sampling != None and dataset.data_source.sampling == None:
                del ds.data_source.sampling
            # If the sampling object actually exists then populate it
            if ds.data_source.sampling != None:
                ds.data_source.sampling.kind = dataset.data_source.sampling.__xmlrpc_class__
                merge_parameters(dataset.data_source.sampling, ds.data_source.sampling.parameters, SamplingParameter)
                    
#        # Clean up the sampling link
#        if ds.sampling == None and sampling != None:
#            ds.sampling = Sampling()
#        elif ds.sampling != None and sampling == None:
#            del ds.sampling
#        # If the sampling object actually exists then populate it
#        if ds.sampling != None:
#            ds.sampling.kind = sampling["class"]
#            del sampling["class"]
#            merge_parameters(sampling, ds.sampling.parameters, SamplingParameter)
                
        # If the repo has a method to persist the dataset then call it and record the output
        fn = find_method(self.repo, "persist", "dataset")
        if fn != None:
            ds.repository_id = fn(ds, schema, location)

        self._persist(ds, session)
        return self._getDataset(ds.id, session)

    @method("persist", "region")    
    def persistRegion(self, region, session, cwd):
        points = list(region.region_points)
        reg = Region()
        if region.id != None:
            reg = session.query(Region).filter(Region.id == region["id"]).one()
        
        reg.name = region.name
        
        while len(reg.region_points) > 0:
            reg.region_points.remove(0)
        i = 0
        for lat,lng in points:
            reg.region_points.append(RegionPoint(lat, lng, i))
            i += 1
        
        return self._persist(reg, session)
    
    @method("persist", "location")    
    def persistLocation(self, location, session, cwd):
        loc = Location()
        copy_attrs(location, loc, ["id", "name", "latitude", "longitude", "elevation"])
        # If the repo has a method to persist the dataset then call it and record the output
        fn = find_method(self.repo, "persist", "location")
        if fn != None:
            loc.repository_id = fn(loc)

        return self._persist(loc, session)
    
    @method("persist", "dataset_metadata_schema")
    def persistDatasetMetaDataSchema(self, schema, session, cwd):
        return self._persistSchema(schema, "dataset_metadata", session)

    @method("persist", "data_entry_schema")
    def persistDataEntrySchema(self, schema, session, cwd):
        return self._persistSchema(schema, "data_entry", session)

    @method("persist", "schema")
    def persistGenericSchema(self, schema, session, cwd):
        return self._persistSchema(schema, "schema", session)
        
    def _persistSchema(self, schema, for_, s):
        if schema.id != None:
            raise PersistenceError("Updates are not supported for Schemas")
        
        attrs = []
        for (key, attr) in schema.attrs.items():
            new_attr = SchemaAttribute()
            new_attr.kind = attr.__xmlrpc_class__
            new_attr.name = attr.name
            new_attr.description = attr.description
            new_attr.units = attr.units
            attrs.append(new_attr)

        parents = list(schema.extends)
        
        schema_ = Schema()
        schema_.name = schema.name
        schema_.attributes = attrs
        
        # Set foreign keys
        if len(parents) > 0:
            attributes = [attr.name for attr in schema_.attributes]
            db_parents = s.query(Schema).filter(Schema.id.in_(parents)).all()
            
            if len(db_parents) != len(parents):
                raise PersistenceError("Could not find all parents")
            # Check parents are of the correct type
            for parent in db_parents:
                if parent.for_ != "schema" and parent.for_ != for_: 
                    raise PersistenceError("Parent %d of different type to ingested schema"%(parent.id))
                for parent_attr in parent.attributes:
                    if parent_attr.name in attributes:
                        raise PersistenceError("Duplicate attribute definition %s from parent %d"%(parent_attr.name, parent.id))
                    attributes.append(parent_attr.name)
                schema_.extends.append(parent)
            
        # Set the schema type
        schema_.for_ = for_

        # If the repo has a method to persist the dataset then call it and record the output
        fn = find_method(self.repo, "persist", "schema")
        if fn != None:
            schema_.repository_id = fn(schema_)

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
            return dao_to_domain(obj)
        except Exception, e:
            logger.error("Error saving: " + str(e))
            session.rollback()
            raise Exception("Could not save dataset:"+ str(e))
            
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
            return dao_to_domain(obj)
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
        
    def getActiveDatasets(self, kind=None):
        """Returns all enabled datasets."""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            objs = s.query(Dataset)
            if kind != None:
                objs = objs.join(Dataset.data_source).filter(Dataset.enabled == True, DataSource.kind == kind).all()
            else:
                objs = objs.filter(Dataset.enabled == True).all()
            ret_list = []
            for obj in objs:
                for attr in get_properties(obj):
                    logger.info("%s=%s"%(attr, getattr(obj, attr)))
                ret = dao_to_domain(obj)
                ret_list.append(ret)
            return ret_list
        except NoResultFound, e:
            return []
        finally:
            s.close()
    
    def getSchema(self, s_id):
        """Get the schema as a DTO"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Schema).filter(Schema.id == s_id).one()
            schema = dao_to_domain(obj)
            return schema
        finally:
            session.close()
            
    def getLocation(self, loc_id):
        """Get the location as a DTO"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Location).filter(Location.id == loc_id).one()
            return dao_to_domain(obj)
        finally:
            session.close()

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
            
    def persistSamplerState(self, ds_id, state):
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(SamplerState).filter(SamplerState.dataset_id == ds_id).all()
            
            state = state.copy()
            to_del = []
            
            for obj in objs:
                prop_name = getattr(obj,"name")
                if prop_name in state:
                    # Update
                    setattr(obj, "value", state[prop_name])
                    del state[prop_name]
                else:
                    # Delete pending
                    to_del.append(obj)
            # Delete
            for obj in to_del:
                session.delete(obj)
            # Add
            for k in state:
                obj = SamplerState()
                obj.dataset_id = ds_id
                setattr(obj, "name", k)
                setattr(obj, "value", state[k])
                session.add(obj)
            session.commit()
            session.flush()
        finally:
            session.close()
    
    def getSamplerState(self, ds_id):
        """Gets the sampler state as a dict. All values will be string."""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(SamplerState).filter(SamplerState.dataset_id == ds_id).all()
            return parameters_to_dict(objs)
        finally:
            session.close()

    def persistDataSourceState(self, ds_id, state):
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(DataSourceState).filter(DataSourceState.dataset_id == ds_id).all()
            
            state = state.copy()
            to_del = []
            
            for obj in objs:
                prop_name = getattr(obj,"name")
                if prop_name in state:
                    # Update
                    setattr(obj, "value", state[prop_name])
                    del state[prop_name]
                else:
                    # Delete pending
                    to_del.append(obj)
            # Delete
            for obj in to_del:
                session.delete(obj)
            # Add
            for k in state:
                obj = DataSourceState()
                obj.dataset_id = ds_id
                setattr(obj, "name", k)
                setattr(obj, "value", state[k])
                session.add(obj)
            session.commit()
            session.flush()
        finally:
            session.close()

    def getDataSourceState(self, ds_id):
        """Gets the data source state as a dict. All values will be string."""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(DataSourceState).filter(DataSourceState.dataset_id == ds_id).all()
            return parameters_to_dict(objs)
        finally:
            session.close()

    def findDatasets(self, **kwargs):
        """Find all datasets with the provided attributes"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(Dataset).all()
            ret_list = []
            for obj in objs:
                ret_list.append(dao_to_domain(obj))
            return ret_list
        finally:
            session.close()

    def findObservations(self, d_id):
        return self.repo.findObservations(self.getDataset(d_id))

    def getDataEntry(self, dataset_id, data_entry_id):
        return self.repo.getDataEntry(dataset_id, data_entry_id)

    def getDataEntryStream(self, dataset_id, data_entry_id, attr):
        return self.repo.getDataEntryStream(dataset_id, data_entry_id, attr)

    @method("persist", "data_entry")
    def persistDataEntry(self, data_entry, session, cwd):
        """Persist the observation to the repository. This method is also responsible for 
        notifying the ingester of any new data, such that triggers can be invoked.
        
        Just calls main method, as we don't need a session.
        
        :param dataset: A dataset dict for the target dataset
        :param obs: Dict of attributes to ingest
        :param cwd: Working directory for this ingest
        """
        if data_entry.timestamp == None: 
            raise ValueError("timestamp is not set")

        dataset_id = data_entry.dataset
        dataset = self.getDataset(dataset_id)
        schema = self.getSchema(dataset.schema)

        obs = self.repo.persistDataEntry(dataset, schema, data_entry, cwd)
        for listener in self.obs_listeners:
            listener.notifyNewObservation(obs, cwd)
        return obs

    def runIngester(self, d_id):
        """Run the ingester for the given dataset ID"""
        self.ingester.queue(self.getDataset(d_id))

    @method("persist", "dataset_metadata_entry")
    def persistDatasetMetadata(self, dataset_metadata, session, cwd):
        dataset_id = dataset_metadata.object_id
        dataset = self.getDataset(dataset_id)
        schema = self.getSchema(dataset_metadata.metadata_schema)
        return self.repo.persistDatasetMetadata(dataset, schema, dataset_metadata.data, cwd)

    @method("persist", "data_entry_metadata_entry")
    def persistDataEntryMetadata(self, data_entry_metadata, session, cwd):
        dataset_id = data_entry_metadata.object_id
        data_entry = self.getDataEntry(dataset_id)
        schema = self.getSchema(data_entry_metadata.metadata_schema)
        return self.repo.persistDataEntryMetadata(data_entry, schema, data_entry_metadata.data, cwd)

    def search(self, object_type, criteria=None):
        where = []
        obj_type = None
        if object_type == "dataset":
            obj_type = Dataset
        elif object_type == "data_entry_schema":
            obj_type = Schema
            where.append(Schema.for_ == "data_entry")
        elif object_type == "dataset_metadata_schema":
            obj_type = Schema
            where.append(Schema.for_ == "dataset_metadata")
        elif object_type == "location":
            obj_type = Location
        if obj_type == None:
            raise ValueError("object_type==%s is not supported"%object_type)
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            objs = s.query(obj_type).filter(*where).all()
            ret_list = []
            for obj in objs:
                ret_list.append(dao_to_domain(obj))
            return ret_list
        finally:
            s.close()
    