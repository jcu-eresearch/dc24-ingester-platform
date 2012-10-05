"""
Created on Oct 5, 2012

@author: nigel
"""
from dc24_ingester_platform.service import IIngesterService
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DECIMAL
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
        if type(getattr(obj, attr)) in (str, int, float):
            ret[attr] = getattr(obj, attr)
        elif type(getattr(obj, attr)) == decimal.Decimal:
            ret[attr] = float(getattr(obj, attr))
    if klass != None: ret["class"] = klass
    return ret

def dict_to_object(dic, obj):
    for attr in dir(obj):
        if attr.startswith("_"): continue
        if dic.has_key(attr): setattr(obj, attr, dic[attr])

class Dataset(Base):
    __tablename__ = "DATASET"
    id = Column(Integer, primary_key=True)
    latitude = Column(DECIMAL)
    longitude = Column(DECIMAL)
    
    
class IngestServiceDB(IIngesterService):
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Dataset.metadata.create_all(self.engine, checkfirst=True)
    
    def persistDataset(self, dataset):
        ds = Dataset()
        dict_to_object(dataset, ds)
        
        s = sessionmaker(bind=self.engine)()
        try:
            s.add(ds)
            s.flush()
            s.commit()
            return obj_to_dict(ds, klass="dataset")
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
        
