"""
This module contains the web controller for the Push Ingest endpoint.
"""

import time
import shutil
import os
from twisted.web.resource import Resource

class PushController(Resource):
    isLeaf = True

    def __init__(self, service, path):
        Resource.__init__(self)
        self.service = service
        self.path = path

    def render_POST(self, request):
        """On post get the ingest key from the path.
        Then, store the post body for ingest.
        """
        if len(request.postpath) != 1:
            request.setResponseCode(400)
            return "Invalid request"
        key = request.postpath[0]
        
        # Get the dataset
        dataset = self.service.getDataset(key)
        
        path = os.path.join(self.path, key)
        if not os.path.exists(path): os.makedirs(path)
        with open(os.path.join(path, str(int(time.mktime(time.gmtime())))), "wb") as f:
            shutil.copyfileobj(request.content, f)
        
        # Enqueue the data
        self.ingester.queue(dataset, {"path":path})
        
        return "OK"

def makePushService(service, staging_dir):
    """Create a push endpoint for Twisted, using the service facade and 
    staging directory provided.
    
    :param service: service facade
    :param staging_dir: the directory under which all ingester folders will be stored.
    """
    return PushController(service, staging_dir)
