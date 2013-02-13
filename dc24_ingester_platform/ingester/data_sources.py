"""
Created on Nov 1, 2012

@author: nigel
"""
import os
import re
import datetime
import calendar
import shutil
import logging
import urllib2
import urlparse
import sys
import json
import pprint
from dc24_ingester_platform.ingester.sos import SOS, SOSVersions, create_namespace_dict, SOSMimeTypes
from lxml import etree

from dc24_ingester_platform.utils import *
from dc24_ingester_platform import IngesterError
from jcudc24ingesterapi.ingester_platform_api import get_properties, Marshaller
from jcudc24ingesterapi.models.data_entry import DataEntry, FileObject
from jcudc24ingesterapi.models.data_sources import _DataSource
from dc24_ingester_platform.ingester.processor import run_script

logger = logging.getLogger("dc24_ingester_platform.ingester.data_sources")

class DataSource(object):
    """A Sampler is an object that takes a configuration and state
    and uses this to determine whether a dataset is due for a new sample"""
    state = None # Holds the state of the Sampler. This is persisted by the ingester.
    
    def __init__(self, state, parameters, config):
        """
        :param state: State information left over from the last run
        :param parameters: Parameters specific to this current run, ie, triggering event IDs
        :param _DataSource: All the data source configuration information
        """
        self.state = state
        self.parameters = parameters
        for param in get_properties(config):
            setattr(self, param, getattr(config, param))
            
    def fetch(self, cwd, service=None):
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
    recursive = False
    def fetch(self, cwd, service=None):
        """Fetch from a URI using urllib2
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        url = urlparse.urlparse(self.url)
        if not self.recursive:
            return self.fetch_single(cwd)
        elif url.scheme == "ftp":
            return self.fetch_ftp(cwd)
        elif url.scheme in ("http", "https"):
            return self.fetch_http(cwd)
        else:
            raise IngesterError("This scheme is not supported: %s"%url.scheme)

    def fetch_http(self, cwd):
        """Recursively fetch from an HTTP server.
        """ 
        RE_A = re.compile("href=\"(\./){0,1}([0-9A-Za-z\-_\.\:]+)\"")
        req = urllib2.Request(self.url)
        ret = []
        
        since = None
        if "lasttime" in self.state and self.state["lasttime"] != None and len(self.state["lasttime"]) > 0:
            since = eut.formatdate(calendar.timegm(parse_timestamp(self.state["lasttime"]).timetuple()), usegmt=True)
        
        f_in = None
        try:
            f_index = urllib2.urlopen(req)
            index_page = f_index.read()
            f_index.close()
            urls = RE_A.findall(index_page)
            found = 0
            
            for url_part in urls:
                url = urlparse.urljoin(self.url, url_part[0]+url_part[1])
                req = urllib2.Request(url)
                if since != None: req.add_header("If-Modified-Since", since)
                try:
                    f_in = urllib2.urlopen(req)
                    f_out_name = os.path.join(cwd, "outputfile%d"%found)
                    timestamp = parse_timestamp_rfc_2822(f_in.headers["Last-Modified"])
                    with file(f_out_name, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    new_data_entry = DataEntry(timestamp=timestamp)
                    new_data_entry[self.field] = FileObject(f_path="outputfile%d"%found, mime_type="" )
                    ret.append(new_data_entry)
                    found += 1
                    
                    if since == None or timestamp > since:
                        since = timestamp
                    
                except urllib2.HTTPError, e:
                    if e.code == 304: 
                        continue
        finally:
            if f_in != None: f_in.close()
            
        self.state["lasttime"] = format_timestamp(since) if since != None else None
        return ret
        
    def fetch_single(self, cwd):
        """Fetch a single resource from a URL"""
        req = urllib2.Request(self.url)
        f_out_name = os.path.join(cwd, "outputfile")
        f_in = None
        try:
            f_in = urllib2.urlopen(req)
            timestamp = parse_timestamp_rfc_2822(f_in.headers["Last-Modified"]) if "Last-Modified" in f_in.headers \
                else datetime.datetime.now()
            with file(f_out_name, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
                
            self.state["lasttime"] = format_timestamp(timestamp)
        finally:
            if f_in != None: f_in.close()
        new_data_entry = DataEntry(timestamp=timestamp)
        new_data_entry[self.field] = FileObject(f_path="outputfile", mime_type="" )
            
        return [new_data_entry]

class PushDataSource(DataSource):
    """Scan an incoming directory for new data. The filename encodes
    the timestamp that should be on the record.
    """
    field = None # The field to ingest into

    def fetch(self, cwd, service=None):
        """Scans a folder to find new files. The filenames are UTC timestamps that used
        as the timestamp for these samples.
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        if not hasattr(self, "path"):
            raise DataSourceError("Path not set")
        if not os.path.exists(self.path):
            raise DataSourceError("Could not find the staging path")
        
        RE_FILENAME = re.compile("^([0-9]+)$")
        ret = []
        for f_name in os.listdir(self.path):
            m = RE_FILENAME.match(f_name)
            if m == None: continue
            new_filename = "file-"+f_name
            os.rename(os.path.join(self.path, f_name), os.path.join(cwd, new_filename))
            timestamp = datetime.datetime.utcfromtimestamp(int(m.group(1)))
            new_data_entry = DataEntry(timestamp=timestamp)
            new_data_entry[self.field] = FileObject(f_path=new_filename, mime_type="" )
            ret.append(new_data_entry)
            
        return ret

class DatasetDataSource(DataSource):
    """Fetches data from a remote data source and returns its data entry.
    This is worked on by the processor script to transform it into
    one or more data entries that conform to the target dataset schema.
    """
    dataset_id = None # Source dataset

    def fetch(self, cwd, service):
        """Extract the observation from the repo.
        
        :param cwd: working directory to place binary data
        :returns: dict containing the data to be ingested
        """
        data_entry = service.getDataEntry(int(self.parameters["dataset"]), int(self.parameters["id"]))
        for k in data_entry.data:
            if isinstance(data_entry.data[k], FileObject):
                dst_file = os.path.join(cwd, k)
                f_in=service.getDataEntryStream(int(self.parameters["dataset"]), int(self.parameters["id"]), k)
                with open(dst_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                f_in.close()
                data_entry.data[k].f_path = k
        return [data_entry]

class SOSScraperDataSource(DataSource):
    def fetch(self, cwd):
        sos = SOS(self.url, SOSVersions.v_1_0_0)
        caps = sos.getCapabilities(["ALL"])

        if self.state is None:
            self.state={}
        if 'sensorml' not in self.state:
            self.state['sensorml'] = []
        if 'observations' not in self.state:
            self.state['observations'] = []
        ret = []
        self.fetch_sensorml(sos, caps, cwd, ret)
        self.fetch_observations(sos, caps, cwd, ret)

        return ret

    def fetch_sensorml(self, sos, caps, cwd, ret):
        namespaces = create_namespace_dict()
        allowed = caps.xpath("/sos:Capabilities/ows:OperationsMetadata/ows:Operation[@name='InsertObservation']"+
                             "/ows:Parameter[@name='AssignedSensorId']/ows:AllowedValues", namespaces=namespaces)
        if len(allowed) != 1:
            raise DataSourceError("AssignedSensorId from xpath match, found: %s"%len(allowed))
        sensorIDS = [x.text for x in allowed[0].xpath("ows:Value", namespaces=namespaces)]

        sensorml_dir = os.path.join(cwd,"sensorml")
        if not os.path.exists(sensorml_dir):
            os.makedirs(sensorml_dir)

        for sensorID in sensorIDS:
            if sensorID not in self.state['sensorml']:
                sml = sos.describeSensor(sensorID)
                sml_path = os.path.join(sensorml_dir, sensorID)
                with open(sml_path, "wb") as sensorml:
                    sensorml.write(etree.tostring(sml,pretty_print=True))
                    timestamp = datetime.datetime.now()
                    new_data_entry = DataEntry(timestamp=timestamp)
                    new_data_entry[self.field] = FileObject(f_path=sml_path, mime_type=SOSMimeTypes.sensorML_1_0_1 )
                    ret.append(new_data_entry)
                self.state['sensorml'].append(sensorID)


    def fetch_observations(self, sos, caps, cwd, ret):
        namespaces = create_namespace_dict()
        obs_range = caps.xpath("/sos:Capabilities/ows:OperationsMetadata/ows:Operation[@name='GetObservationById']"+
                               "/ows:Parameter/ows:AllowedValues/ows:Range", namespaces=namespaces)
        insert_dir = os.path.join(cwd, "insert")
        if not os.path.exists(insert_dir):
            os.makedirs(insert_dir)

        for _range in obs_range:
            min = _range.xpath("ows:MinimumValue", namespaces=namespaces)
            max = _range.xpath("ows:MaximumValue", namespaces=namespaces)
            if len(min) != 1:
                raise DataSourceError("Only 1 ows:MinimumValue expected, %s found."%len(min))
            if len(max) != 1:
                raise DataSourceError("Only 1 ows:MaximumValue expected, %s found."%len(max))
            for i in range(int(min[0].text), int(max[0].text) + 1):
                observationID = "o_%s"%i
                if observationID not in self.state['observations']:
                    sos_obs = sos.getObservationByID(observationID, "om:Observation")
                    obs_path = os.path.join(insert_dir, "%s.xml"%i)
                    with open(obs_path, "wb") as output:
                        output.write(etree.tostring(sos_obs,pretty_print=True))
                        timestamp = datetime.datetime.now()
                        new_data_entry = DataEntry(timestamp=timestamp)
                        new_data_entry[self.field] = FileObject(f_path=obs_path, mime_type=SOSMimeTypes.sensorML_1_0_1 )
                        ret.append(new_data_entry)
                    self.state['observations'].append(observationID)



data_sources = {
    "pull_data_source":PullDataSource,
    "push_data_source":PushDataSource,
    "dataset_data_source":DatasetDataSource,
    "sos_scraper_data_source": SOSScraperDataSource
}

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
    if data_source_config.__xmlrpc_class__ not in data_sources:
        raise NoSuchDataSource("Sampler '%s' not found"%(data_source_config.__xmlrpc_class__))
    return data_sources[data_source_config.__xmlrpc_class__](state, parameters, data_source_config)

def main():
    args = sys.argv
    if len(args) not in (3,4):
        print "Usage: %s <config file> <working directory>"%(args[0])
        print """Where config file contains:
        {
        "class":"INGESTER_CLASS",
        "state":{...},
        "parameters":{...},
        "config":{...}
        }"""
        return(1)
    
    cfg_file = args[1]
    cwd = args[2]
    
    script = args[3] if len(args) > 3 else None
    
    # Validate parameters
    if not os.path.exists(cfg_file):
        print "Config file not found: %s"%(cfg_file)
        return(1) 
    if not os.path.exists(cwd) and os.path.isdir(cwd):
        print "Working directory does not exist"
        return(1)
    with open(sys.argv[1], "r") as f:
        cfg = json.load(f)
    if "class" not in cfg or "state" not in cfg or "parameters" not in cfg or "config" not in cfg:
        print "Config file not valid"
        return(1)
    
    # Create config object
    m = Marshaller()

    data_source_do = m.class_for(cfg["class"])()
    for k in cfg["config"]:
        setattr(data_source_do, k, cfg["config"][k])

    data_source = create_data_source(data_source_do, cfg["state"], cfg["parameters"])
    
    results = data_source.fetch(cwd)
    print "Initial results"
    print "---------------"
    for result in results:
        print str(result)
        
    if script != None:
        with open(script) as f:
            script = f.read()
        results = run_script(script, cwd, results)
        
        print "Processed results"
        print "-----------------"
        for result in results:
            print str(result)
    return 0

if __name__ == "__main__":
    sys.exit(main())