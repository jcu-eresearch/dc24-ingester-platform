# Twistd TAC to start management service on port 8080
import os
import sys
import logging
import logging.config
import tempfile

from twisted.application import service, internet
from twisted.web import static, server
from twisted.web.resource import Resource
from twisted.python import log

# Setup logging before anything else gets imported
logging.config.fileConfig("logging.conf")

observer = log.PythonLoggingObserver()
observer.start()

from dc24_ingester_platform import mock
from dc24_ingester_platform import ingester

service_facade = mock.makeMockService()

# attach the service to its parent application
application = service.Application("DC24 Mock Ingester Platform")
root = Resource()
root.putChild("api", mock.makeMockServer())
service = internet.TCPServer(8080, server.Site(root))
service.setServiceParent(application)

ingester.startIngester(service_facade, tempfile.gettempdir(), mock.data_source_factory)


