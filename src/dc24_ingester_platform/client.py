"""
Ingest Platform management client

This module will contain a CLI client for the management interface XMLRPC service.

Created on Oct 3, 2012

@author: Nigel Sim <nigel.sim@coastalcoms.com>
"""
import sys
import xmlrpclib
import getopt

class Client(object):
    def __init__(self, server):
        self.server = xmlrpclib.ServerProxy(server)
        
    def getMethods(self):
        return []
        
    def getDatasets(self):
        return []

def main():
    """Main entry point. Exposed as a function so that it is compatible with the egg script process"""
    opts, args = getopt.getopt(sys.argv[1:], "s:")
    server_url = None
    for k,v in opts:
        if k == "-s":
            server_url = v
    
    errors = ""
    if server_url == None:
        errors += "No server URL set"
        
    if len(errors) > 0:
        print errors
        sys.exit(1)

if __name__ == "__main__":
    main()
        
    