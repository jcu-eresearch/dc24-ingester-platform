"""
Created on Nov 1, 2012

@author: nigel
"""
import os
import re
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
    
    def __init__(self, state, parameters, **kwargs):
        self.state = state
        self.parameters = parameters
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def fetch(self, cwd):
        """Downloads and curate data from data source.
        
        :param cwd: working directory to place binary data
        :returns: array of dicts containing the data to be ingested
        """
        raise NotImplementedError("sample is not implemented for "+str(type(self)))


class PullDataSource(DataSource):
    """The pull data source fetches from a URL and ingests into the configured
    field. It stores the last timestamp to determine if there is new data
    """
    field = None # The field to ingest into
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
        return [{"time":timestamp, self.field: "outputfile"}]

class PushDataSource(DataSource):
    """Scan an incoming directory for new data. The filename encodes
    the timestamp that should be on the record.
    """
    field = None # The field to ingest into

    def fetch(self, cwd):
        """Scans a folder to find new files. The filenames are UTC timestamps that used
        as the timestamp for these samples.
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        if "path" not in self.parameters:
            raise DataSourceError("Path not in the parameter list")
        if not os.path.exists(self.parameters["path"]):
            raise DataSourceError("Could not find the staging path")
        
        RE_FILENAME = re.compile("^([0-9]+)$")
        ret = []
        for f_name in os.listdir(self.parameters["path"]):
            m = RE_FILENAME.match(f_name)
            if m == None: continue
            new_filename = "file-"+f_name
            os.rename(os.path.join(self.parameters["path"], f_name), os.path.join(cwd, new_filename))
            timestamp = format_timestamp(datetime.datetime.utcfromtimestamp(int(m.group(1))))
            ret.append({"time":timestamp, self.field: new_filename})
        return ret

class DatasetDataSource(DataSource):
    """Fetches data from a remote data source and returns its data entry.
    This is worked on by the processor script to transform it into
    one or more data entries that conform to the target dataset schema.
    """
    dataset = None # Source dataset
    data_entry = None # Source data entry

    def fetch(self, cwd):
        """Fetch from a URI using urllib2
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        return [self.data_entry]

data_sources = {"pull_data_source":PullDataSource, "push_data_source":PushDataSource, "dataset_data_source":DatasetDataSource}

class NoSuchDataSource(Exception):
    """An exception that occurs when there is no sampler available."""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class DataSourceError(Exception):
    """An exception that occurs when there is an error executing the data source."""
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def create_data_source(data_source_config, state, parameters):
    """Create the correct configured sampler from the provided dict"""
    if data_source_config["class"] not in data_sources:
        raise NoSuchDataSource("Sampler '%s' not found"%(data_source_config["class"]))
    args = dict(data_source_config)
    del args["class"]
    return data_sources[data_source_config["class"]](state, parameters, **args)
