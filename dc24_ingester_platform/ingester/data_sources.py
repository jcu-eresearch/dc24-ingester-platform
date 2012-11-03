"""
Created on Nov 1, 2012

@author: nigel
"""
import os
import datetime
import shutil
import logging
import urllib2
from dc24_ingester_platform.utils import *

logger = logging.getLogger("dc24_ingester_platform.ingester.data_sources")

class DataSource(object):
    """A Sampler is an object that takes a configuration and state
    and uses this to determine whether a dataset is due for a new sample"""
    state = None # Holds the state of the Sampler. This is persisted by the ingester.
    
    def __init__(self, state, **kwargs):
        self.state = {}
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def fetch(self, cwd):
        """Downloads and curate data from data source.
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        raise NotImplementedError("sample is not implemented for "+str(type(self)))


class PullDataSource(DataSource):
    def fetch(self, cwd):
        """Fetch from a URI using urllib2
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        req = urllib2.Request(self.uri)
        f_out_name = os.path.join(cwd, "outputfile")
        f_in = None
        try:
            f_in = urllib2.urlopen(req)
            timestamp = format_timestamp(f_in.headers["Last-Modified"]) if "Last-Modified" in f_in.headers \
                else format_timestamp(datetime.datetime.now())
            with file(f_out_name, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
                
            self.state["lasttime"] = timestamp
        finally:
            if f_in != None: f_in.close()
        return {"time":timestamp, self.field: "outputfile"}

data_sources = {"pull_data_source":PullDataSource}

class NoSuchDataSource(Exception):
    """An exception that occurs when there is no sampler available."""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

def create_data_source(data_source_config, state):
    """Create the correct configured sampler from the provided dict"""
    if data_source_config["class"] not in data_sources:
        raise NoSuchDataSource("Sampler '%s' not found"%(data_source_config["class"]))
    args = dict(data_source_config)
    del args["class"]
    return data_sources[data_source_config["class"]](state, **args)