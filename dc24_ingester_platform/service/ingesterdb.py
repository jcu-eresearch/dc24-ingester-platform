"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService, find_method, method
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

import jcudc24ingesterapi.models.locations
import jcudc24ingesterapi.models.dataset
import jcudc24ingesterapi.schemas.data_types
import jcudc24ingesterapi.models.system
from jcudc24ingesterapi.search import DataEntrySearchCriteria, DatasetSearchCriteria, DataEntryMetadataSearchCriteria, \
            DatasetMetadataSearchCriteria, LocationSearchCriteria, DataEntrySchemaSearchCriteria, \
            DataEntryMetadataSchemaSearchCriteria, DatasetMetadataSchemaSearchCriteria, SearchResults
from jcudc24ingesterapi.schemas import ConcreteSchema
from jcudc24ingesterapi.models.locations import LocationOffset
from jcudc24ingesterapi.ingester_platform_api import get_properties, Marshaller
import datetime
from jcudc24ingesterapi.models.data_sources import DatasetDataSource
from jcudc24ingesterapi.ingester_exceptions import PersistenceError, \
    InvalidObjectError, StaleObjectError
from sqlalchemy.types import TEXT
import json
from jcudc24ingesterapi import ValidationError

logger = logging.getLogger(__name__)

Base = declarative_base()

domain_marshaller = Marshaller()

class Region(Base):
    __tablename__ = "REGION"
    __xmlrpc_class__ = "region"
    id = Column(Integer, primary_key=True)
    version = Column(Integer, nullable=False, default=1)
    name = Column(String(255))
    # parentRegions = orm.relationship("Region")
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
    version = Column(Integer, nullable=False, default=1)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    name = Column(String(255))
    elevation = Column(DECIMAL)
    repository_id = Column(String(255))
    # region = orm.relationship("Region", uselist=False)

class Dataset(Base):
    __tablename__ = "DATASETS"
    __xmlrpc_class__ = "dataset"
    id = Column(Integer, primary_key=True)
    version = Column(Integer, nullable=False, default=1)
    location = Column(Integer, ForeignKey('LOCATIONS.id'))
    data_source = orm.relationship("DataSource", uselist=False)
    schema = Column(Integer, ForeignKey('SCHEMA.id'))
    enabled = Column(Boolean, default=True)
    running = Column(Boolean, default=False)
    description = Column(String(255))
    redbox_uri = Column(String(255))
    repository_id = Column(String(255))
    # FIXME: Move to separate schema
    x = Column(DECIMAL)
    y = Column(DECIMAL)
    z = Column(DECIMAL)

class Sampling(Base):
    """A DataSource is a generic data storage class"""
    __tablename__ = "SAMPLING"
    __xmlrpc_class__ = "sampling"
    id = Column(Integer, primary_key=True)
    kind = Column(String(255))
    data_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))
    parameters = orm.relationship("SamplingParameter")

class SamplingParameter(Base):
    __tablename__ = "SAMPLING_PARAMETERS"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    value = Column(String(255))
    sampling_id = Column(Integer, ForeignKey("SAMPLING.id"))
    
class DataSource(Base):
    """A DataSource is a generic data storage class"""
    __tablename__ = "DATA_SOURCES"
    __xmlrpc_class__ = "data_source"
    id = Column(Integer, primary_key=True)
    kind = Column(String(255))
    sampling = orm.relationship("Sampling", uselist=False)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
    parameters = orm.relationship("DataSourceParameter")
    processing_script = Column(String(32000))

class DataSourceParameter(Base):
    __tablename__ = "DATA_SOURCE_PARAMETERS"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    value = Column(String(255))
    dataset_source_id = Column(Integer, ForeignKey("DATA_SOURCES.id"))

schema_to_schema = Table("schema_to_schema", Base.metadata,
    Column("child_id", Integer, ForeignKey("SCHEMA.id"), primary_key=True),
    Column("parent_id", Integer, ForeignKey("SCHEMA.id"), primary_key=True)
)

class Schema(Base):
    __tablename__ = "SCHEMA"
    __xmlrpc_class__ = "schema"
    id = Column(Integer, primary_key=True)
    version = Column(Integer, nullable=False, default=1)
    name = Column(String(255))
    for_ = Column(String(255), name="for")
    attributes = orm.relationship("SchemaAttribute")
    repository_id = Column(String(255))
    extends = relationship("Schema",
        secondary=schema_to_schema,
        primaryjoin=id == schema_to_schema.c.child_id,
        secondaryjoin=id == schema_to_schema.c.parent_id)
    
class SchemaAttribute(Base):
    __tablename__ = "SCHEMA_ATTRIBUTE"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    kind = Column(String(255))
    units = Column(String(255))
    description = Column(String(255))
    schema_id = Column(Integer, ForeignKey("SCHEMA.id"))

class IngesterLog(Base):
    __tablename__ = "INGESTER_LOG"
    __xmlrpc_class__ = "ingester_log"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    level = Column(String(255))
    message = Column(String(255))
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))

class SamplerState(Base):
    __tablename__ = "SAMPLER_STATE"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    value = Column(String(255))
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
 
class DataSourceState(Base):
    __tablename__ = "DATA_SOURCE_STATE"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    value = Column(String(255))
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))   
    
class ObjectHistory(Base):
    __tablename__ = "OBJECT_HISTORY"
    id = Column(Integer, primary_key=True)
    object = Column(String(255), nullable=False)
    object_id = Column(Integer, nullable=False)
    version = Column(String(255), nullable=False)
    data = Column(TEXT)
    timestamp = Column(DateTime)
    
class IngesterTask(Base):
    __tablename__ = "INGESTER_TASK"
    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("DATASETS.id"))
    dataset = orm.relationship("Dataset", uselist=False)
    timestamp = Column(DateTime)
    state = Column(Integer, nullable=False, default=0)
    cwd = Column(String(255), nullable=False)
    parameters = Column(TEXT)
    
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
        prop_name = getattr(obj, name_attr)
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
    FIXME this needs to properly order ingest such that dataset data sources are correctly ingested last.
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
        copy_attrs(dao, domain, ["id", "version", "name"])
        domain.region_points = [(float(p.latitude), float(p.longitude)) for p in dao.region_points]
    elif type(dao) == Schema:
        domain = domain_marshaller.class_for(dao.for_ + "_schema")()
        
        copy_attrs(dao, domain, ["id", "version", "name", "repository_id"])
        domain.extends.extend([e.id for e in dao.extends])
        for attr in dao.attributes:
            attr_ = None
            if attr.kind == "file": attr_ = jcudc24ingesterapi.schemas.data_types.FileDataType(attr.name)
            elif attr.kind == "string": attr_ = jcudc24ingesterapi.schemas.data_types.String(attr.name)
            elif attr.kind == "integer": attr_ = jcudc24ingesterapi.schemas.data_types.Integer(attr.name)
            elif attr.kind == "double": attr_ = jcudc24ingesterapi.schemas.data_types.Double(attr.name)
            elif attr.kind == "datetime": attr_ = jcudc24ingesterapi.schemas.data_types.DateTime(attr.name)
            elif attr.kind == "boolean": attr_ = jcudc24ingesterapi.schemas.data_types.Boolean(attr.name)
            else: raise PersistenceError("Invalid data type: %s" % attr.kind)
            attr_.units = attr.units
            attr_.description = attr.description
            domain.addAttr(attr_)
    elif type(dao) == Location:
        domain = jcudc24ingesterapi.models.locations.Location()
        copy_attrs(dao, domain, ["id", "version", "name", "latitude", "longitude", "elevation", "repository_id"])
    elif type(dao) == Dataset:
        domain = jcudc24ingesterapi.models.dataset.Dataset()
        copy_attrs(dao, domain, ["id", "version", "location", "schema", "enabled", "description", "redbox_uri", "repository_id"])
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
    elif type(dao) == IngesterLog:
        domain = jcudc24ingesterapi.models.system.IngesterLog()
        copy_attrs(dao, domain, get_properties(domain))
    else:
        raise PersistenceError("Could not convert DAO object to domain: %s" % (str(type(dao))))
    return domain

def sort_datasets(datasets):
    """This method reorders the list so that datasets that have dataset data sources are ingested after there
    dependent data source.
    """
    ingested = []
    output = []
    remaining = []
    while len(datasets) > 0:
        for dataset in datasets:
            if dataset.__xmlrpc_class__ != "dataset":
                output.append(dataset)
                continue
            if dataset.data_source == None or \
                    dataset.data_source.__xmlrpc_class__ != "dataset_data_source" or \
                    dataset.data_source.dataset_id == None or \
                    dataset.data_source.dataset_id > 0 or \
                    dataset.data_source.dataset_id in ingested:
                ingested.append(dataset.id)
                output.append(dataset)
            else:
                remaining.append(dataset)
        if len(remaining) > 0 and len(remaining) == len(datasets):
            raise Exception("Can not resolve ingestion order")
        datasets = remaining
            
    return output

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
            
            unit._to_insert = sort_datasets(unit._to_insert)
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
                        if obj.data_source != None and isinstance(obj.data_source, DatasetDataSource) and obj.data_source.dataset_id < 0:
                            obj.data_source.dataset_id = datasets[obj.data_source.dataset_id]
                        
                    elif cls.endswith("schema"):
                        obj.extends = [ schemas[p_id] if p_id < 0 else p_id for p_id in obj.extends]
                            
                    fn = find_method(self, "persist", cls)
                    if fn == None:
                        raise ValueError("Could not find method for", "persist", cls)
                    obj = fn(obj, s, cwd)
                    if cls == "location":
                        locs[oid] = obj.id
                    elif cls.endswith("schema"):
                        schemas[oid] = obj.id
                    elif cls == "dataset":
                        datasets[oid] = obj.id
                            
                    obj.correlationid = oid
                    ret.append(obj)
            for obj_id in unit._to_enable:
                self.enable_dataset(obj_id)
            for obj_id in unit._to_disable:
                self.disable_dataset(obj_id)
            s.commit()
            return ret
        except Exception as e:
            s.rollback()
            raise
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
        raise ValueError("%s not supported" % (cls))

    @method("persist", "dataset")
    def persist_dataset(self, dataset, session, cwd):
        """Assumes that we have a copy of the object, so we can change it if required.
        """
        if dataset.location == None:
            raise ValueError("Location must be set")
        # Check schema is of the correct type

        try:
            location = session.query(Location).filter(Location.id == dataset.location).one()
        except NoResultFound, e:
            raise ValueError("Provided location not found: %d" % dataset.location)
        try:
            schema = session.query(Schema).filter(Schema.id == dataset.schema).one()
        except NoResultFound, e:
            raise ValueError("Provided schema not found: %d" % dataset.schema)
        if schema.for_ != "data_entry":
            raise ValueError("The schema must be for a data_entry")
        
        ds = Dataset()

        if dataset.id != None:
            try:
                ds = session.query(Dataset).filter(Dataset.id == dataset.id, Dataset.version == dataset.version).one()
                self.save_version(session, ds)
            except NoResultFound:
                raise StaleObjectError("No dataset with id=%d and version=%d to update" % (dataset.id, dataset.version))
        
        ds.version = dataset.version + 1 if dataset.version != None else 1
            
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
        return self._get_dataset(ds.id, session)

    @method("persist", "region")    
    def persist_region(self, region, session, cwd):
        points = list(region.region_points)
        reg = Region()
        if region.id != None:
            try:
                reg = session.query(Region).filter(Region.id == region.id, Region.version == region.version).one()
                self.save_version(session, reg)
            except NoResultFound:
                raise StaleObjectError("No region with id=%d and version=%d to update" % (region.id, region.version))
        
        reg.name = region.name
        reg.version = region.version + 1 if region.version != None else 1
        
        points_lookup = {}
        existing = set()
        for point in reg.region_points:
            points_lookup[point.order] = point
            existing.add(point.order)
            
        new_points = set()
        i = 0
        for lat, lng in points:
            new_points.add(i)
            if i in points_lookup:
                points_lookup[i].latitude = lat
                points_lookup[i].longitude = lng
            else:
                reg.region_points.append(RegionPoint(lat, lng, i))
            i += 1
        # Now clean up unneeded point
        for i in existing - new_points:
            point = points_lookup[i]
            session.delete(point)
            reg.region_points.remove(point)
        
        return self._persist(reg, session)
    
    @method("persist", "location")    
    def persist_location(self, location, session, cwd):
        loc = Location()
        
        if location.id != None:
            try:
                loc = session.query(Location).filter(Location.id == location.id, Location.version == location.version).one()
                self.save_version(session, loc)
            except NoResultFound:
                raise StaleObjectError("No location with id=%d and version=%d to update" % (location.id, location.version))
        
        loc.version = location.version + 1 if location.version != None else 1
        
        copy_attrs(location, loc, ["id", "name", "latitude", "longitude", "elevation"])
        # If the repo has a method to persist the dataset then call it and record the output
        fn = find_method(self.repo, "persist", "location")
        if fn != None:
            loc.repository_id = fn(loc)

        return self._persist(loc, session)
    
    @method("persist", "dataset_metadata_schema")
    def persist_dataset_metadata_schema(self, schema, session, cwd):
        return self._persist_schema(schema, "dataset_metadata", session)

    @method("persist", "data_entry_metadata_schema")
    def persist_data_entry_metadata_schema(self, schema, session, cwd):
        return self._persist_schema(schema, "data_entry_metadata", session)

    @method("persist", "data_entry_schema")
    def persist_data_entry_schema(self, schema, session, cwd):
        return self._persist_schema(schema, "data_entry", session)

    @method("persist", "schema")
    def persist_generic_schema(self, schema, session, cwd):
        return self._persist_schema(schema, "schema", session)
        
    def _persist_schema(self, schema, for_, s):
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
                    raise PersistenceError("Parent %d of different type to ingested schema" % (parent.id))
                for parent_attr in parent.attributes:
                    if parent_attr.name in attributes:
                        raise PersistenceError("Duplicate attribute definition %s from parent %d" % (parent_attr.name, parent.id))
                    attributes.append(parent_attr.name)
                schema_.extends.append(parent)
            
        # Set the schema type
        schema_.for_ = for_

        # Persist now to get an ID
        ret = self._persist(schema_, s)
        
        # If the repo has a method to persist the dataset then call it and record the output
        fn = find_method(self.repo, "persist", "schema")
        if fn != None:
            schema_.repository_id = fn(schema_)
            return self._persist(schema_, s)
        else:
            return ret
        
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
            raise Exception("Could not save dataset:" + str(e))
            
    def delete_dataset(self, dataset):
        pass
    
    def get_dataset(self, ds_id):
        """Get the dataset as a DTO"""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            return self._get_dataset(ds_id, s)
        finally:
            s.close()
        
    def _get_dataset(self, ds_id, session):
        """Private method to actually get the dataset using the session provided.
        """
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            return dao_to_domain(obj)
        except NoResultFound, e:
            return None
        
    def enable_dataset(self, ds_id):
        """Enable the dataset"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            obj.enabled = True
            session.merge(obj)
            session.commit()
        finally:
            session.close()
    
    def disable_dataset(self, ds_id):
        """Disable the dataset"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            obj.enabled = False
            session.merge(obj)
            session.commit()
        finally:
            session.close()
            
    def create_ingest_task(self, ds_id, cwd, parameters=None):
        """Mark the dataset as currently undertaing the ingest process. This
        will also persist the ingest task and return the id for this object."""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Dataset).filter(Dataset.id == ds_id).one()
            obj.running = True
            session.merge(obj)

            task = IngesterTask()
            task.dataset_id = ds_id
            task.parameters = json.dumps(parameters)
            task.cwd = cwd
            task.state = 0
            task.timestamp = datetime.datetime.utcnow()
            session.add(task)
            
            session.commit()
            
            return task.id
        finally:
            session.close()
    
    def mark_ingress_complete(self, ingest_task_id):
        """Once the ingress is complete the dataset is able to ingress new data
        without conflict"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            task = session.query(IngesterTask).filter(IngesterTask.id == ingest_task_id).one()
            task.state = 1
            session.merge(task)
            
            obj = session.query(Dataset).filter(Dataset.id == task.dataset_id).one()
            obj.running = False
            session.merge(obj)
            
            session.commit()
        finally:
            session.close()
        
    def mark_ingest_complete(self, ingest_task_id):
        """Once the ingest is complete the ingest process is complete"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            task = session.query(IngesterTask).filter(IngesterTask.id == ingest_task_id).one()
            task.state = 2
            session.merge(task)
            
            session.commit()
        finally:
            session.close()
    
    def get_ingest_queue(self):
        """Get all the items queued for ingest.
        :returns: List of tuples (task_id, state, dataset object, parameter dict, cwd)
        """
        session = orm.sessionmaker(bind=self.engine)()
        try:
            ret = session.query(IngesterTask).filter(IngesterTask.state.in_( (0,1) )).all()
            return [(obj.id, obj.state, dao_to_domain(obj.dataset), json.loads(obj.parameters), obj.cwd) for obj in ret]
        finally:
            session.close()

    def get_active_datasets(self, kind=None):
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
                    logger.info("%s=%s" % (attr, getattr(obj, attr)))
                ret = dao_to_domain(obj)
                ret_list.append(ret)
            return ret_list
        except NoResultFound, e:
            return []
        finally:
            s.close()
    
    def get_schema(self, s_id):
        """Get the schema as a DTO"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Schema).filter(Schema.id == s_id).one()
            schema = dao_to_domain(obj)
            return schema
        finally:
            session.close()
            
    def get_schema_tree(self, s_id):
        """Get the schema as a DTO"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            return self._get_schema_tree(session, s_id)
        finally:
            session.close()
            
    def _get_schema_tree(self, session, s_id):
        """Get the schema as a DTO"""
        queue = [s_id]
        ret = []
        while len(queue):
            s_id = queue[0]
            del queue[0]
            
            obj = session.query(Schema).filter(Schema.id == s_id).one()
            schema = dao_to_domain(obj)
            ret.append(schema)
            for extends in schema.extends:
                queue.append(extends)
        return ret
            
    def get_location(self, loc_id):
        """Get the location as a DTO"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            obj = session.query(Location).filter(Location.id == loc_id).one()
            return dao_to_domain(obj)
        finally:
            session.close()

    def log_ingester_event(self, dataset_id, timestamp, level, message):
        """Write a database log entry for an event that occured on a dataset ingest task
        :param dataset_id: id of the dataset the ingest even was occuring on
        """
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
    
    def get_ingester_logs(self, dataset_id):
        """Returns a list of all events that have occurred on a given dataset"""
        s = orm.sessionmaker(bind=self.engine)()
        try:
            objs = s.query(IngesterLog).filter(IngesterLog.dataset_id == dataset_id).all()
            ret_list = []
            for obj in objs:
                ret_list.append(dao_to_domain(obj))
            return ret_list
        finally:
            s.close()
            
    def persist_sampler_state(self, ds_id, state):
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(SamplerState).filter(SamplerState.dataset_id == ds_id).all()
            
            state = state.copy()
            to_del = []
            
            for obj in objs:
                prop_name = getattr(obj, "name")
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
    
    def get_sampler_state(self, ds_id):
        """Gets the sampler state as a dict. All values will be string."""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(SamplerState).filter(SamplerState.dataset_id == ds_id).all()
            return parameters_to_dict(objs)
        finally:
            session.close()

    def persist_data_source_state(self, ds_id, state):
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(DataSourceState).filter(DataSourceState.dataset_id == ds_id).all()
            
            state = state.copy()
            to_del = []
            
            for obj in objs:
                prop_name = getattr(obj, "name")
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

    def get_data_source_state(self, ds_id):
        """Gets the data source state as a dict. All values will be string."""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(DataSourceState).filter(DataSourceState.dataset_id == ds_id).all()
            return parameters_to_dict(objs)
        finally:
            session.close()

    def find_datasets(self, **kwargs):
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

    def find_schemas(self, repository_id=None, **kwargs):
        """Find all schemas matching the provided attributes"""
        session = orm.sessionmaker(bind=self.engine)()
        try:
            objs = session.query(Schema)
            if repository_id != None:
                objs = objs.filter(Schema.repository_id == repository_id)
                
            objs.all()
            
            ret_list = []
            for obj in objs:
                ret_list.append(dao_to_domain(obj))
            return ret_list
        finally:
            session.close()

    def find_data_entries(self, dataset_id):
        """Find the data entries for this dataset. Lookup the dataset domain
        object and pass it to the repo layer.
        """
        return self.repo.find_data_entries(self.get_dataset(dataset_id))

    def get_data_entry(self, dataset_id, data_entry_id):
        return self.repo.get_data_entry(dataset_id, data_entry_id)

    def get_data_entry_stream(self, dataset_id, data_entry_id, attr):
        return self.repo.get_data_entry_stream(dataset_id, data_entry_id, attr)

    @method("persist", "data_entry")
    def persist_data_entry(self, data_entry, session, cwd):
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
        dataset = self.get_dataset(dataset_id)
        schema = ConcreteSchema(self.get_schema_tree(dataset.schema))
        obs = self.repo.persist_data_entry(dataset, schema, data_entry, cwd)
        for listener in self.obs_listeners:
            listener.notify_new_data_entry(obs, cwd)
        return obs

    def invoke_ingester(self, dataset_id):
        """Run the ingester for the given dataset ID. 
        If this is a scheduled dataset then this will be as if the sampler had
        run. But, if it is a dataset data source then we'll go and get all
        the data entries in the source dataset."""
        self.ingester.invoke_ingester(self.get_dataset(dataset_id))

    @method("persist", "dataset_metadata_entry")
    def persist_dataset_metadata(self, dataset_metadata, session, cwd):
        dataset_id = dataset_metadata.object_id
        dataset = self.get_dataset(dataset_id)
        if dataset == None:
            raise InvalidObjectError([ValidationError("dataset", "References non-existant dataset")])
        schema = self.get_schema(dataset_metadata.metadata_schema)
        return self.repo.persist_dataset_metadata(dataset, schema, dataset_metadata.data, cwd)

    @method("persist", "data_entry_metadata_entry")
    def persist_data_entry_metadata(self, data_entry_metadata, session, cwd):
        data_id = data_entry_metadata.object_id
        dataset_id = data_entry_metadata.dataset
        data_entry = self.get_data_entry(dataset_id, data_id)
        if data_entry == None:
            raise InvalidObjectError([ValidationError("data_entry", "References non-existant data entry")])
        schema = self.get_schema(data_entry_metadata.metadata_schema)
        return self.repo.persist_data_entry_metadata(data_entry, schema, data_entry_metadata.data, cwd)

    def search(self, criteria, offset, limit=10):
        where = []
        obj_type = None
        if isinstance(criteria, DataEntrySearchCriteria):
            return self.repo.find_data_entries(self.get_dataset(criteria.dataset), offset=offset,
                            limit=limit, start_time=criteria.start_time, end_time=criteria.end_time)
        elif isinstance(criteria, DatasetMetadataSearchCriteria):
            return self.repo.find_dataset_metadata(self.get_dataset(criteria.dataset), offset=offset,
                            limit=limit)
        elif isinstance(criteria, DataEntryMetadataSearchCriteria):
            return self.repo.find_data_entry_metadata(self.get_data_entry(criteria.dataset, criteria.data_entry),
                            offset=offset, limit=limit)
        elif isinstance(criteria, DatasetSearchCriteria):
            obj_type = Dataset
            if criteria.location != None:
                where.append(Dataset.location == criteria.location)
            if criteria.schema != None:
                where.append(Dataset.schema == criteria.schema)
            
        elif isinstance(criteria, LocationSearchCriteria):
            obj_type = Location
        elif isinstance(criteria, DataEntrySchemaSearchCriteria):
            obj_type = Schema
            where.append(Schema.for_ == "data_entry")
        elif isinstance(criteria, DataEntryMetadataSchemaSearchCriteria):
            obj_type = Schema
            where.append(Schema.for_ == "data_entry_metadata")
        elif isinstance(criteria, DatasetMetadataSchemaSearchCriteria):
            obj_type = Schema
            where.append(Schema.for_ == "dataset_metadata")
        if obj_type == None:
            raise ValueError("object_type==%s is not supported" % str(type(criteria)))
        
        s = orm.sessionmaker(bind=self.engine)()
        try:
            count = s.query(obj_type).filter(*where).count()
            objs = s.query(obj_type).filter(*where).limit(limit).offset(offset).all()
            ret_list = []
            for obj in objs:
                ret_list.append(dao_to_domain(obj))
            return SearchResults(ret_list, offset, limit, count)
        finally:
            s.close() 

    def save_version(self, session, obj):
        """Save a JSON encoded copy of the original object before updating. 
        This method will put the save in the transaction, so that if the transction
        is rolled back, the version save will not occur"""
        
        if not isinstance(obj, (Region, Location, Dataset)):
            logger.error("Tried to save the version of a non versionable object")
            return
        
        history = ObjectHistory()
        history.object = obj.__class__.__name__
        history.object_id = obj.id
        history.version = obj.version
        history.timestamp = datetime.datetime.utcnow()
        history.data = json.dumps(domain_marshaller.obj_to_dict(dao_to_domain(obj)))
        
        session.add(history)
