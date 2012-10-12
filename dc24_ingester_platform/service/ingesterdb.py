"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey
from sqlalchemy.orm import sessionmaker
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
        if type(getattr(obj, attr)) in (str, int, float, unicode):
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
    
class IngesterServiceDB(IIngesterService):
    """This service provides DAO operations for the ingester service.
    
    All objects passed in and out of this service are dicts
    """
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Location.metadata.create_all(self.engine, checkfirst=True)
        Dataset.metadata.create_all(self.engine, checkfirst=True)
    
    def persistDataset(self, dataset):
        ds = Dataset()
        dict_to_object(dataset, ds)
        return self.persist(ds)
        
    def persistLocation(self, dataset):
        loc = Location()
        dict_to_object(dataset, loc)
        return self.persist(loc)
        
    def persist(self, obj):
        s = sessionmaker(bind=self.engine)()
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
        finally:
            s.close()
            
    def deleteDataset(self, dataset):
        pass
    def getDataset(self, id=None):
        s = sessionmaker(bind=self.engine)()
        try:
            return s.query(Dataset).filter(Dataset.id == id).one()
        except NoResultFound, e:
            return None
        finally:
            s.close()
        
