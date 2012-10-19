# Twistd TAC to start management service on port 8080
import os
import sys
import logging
import logging.config

from twisted.application import service, internet
from twisted.web import static, server
from twisted.python import log

# Setup logging before anything else gets imported
logging.config.fileConfig("logging.conf")

observer = log.PythonLoggingObserver()
observer.start()

from dc24_ingester_platform import webservice
from dc24_ingester_platform import ingester
from dc24_ingester_platform import service as platform_service

# attach the service to its parent application
application = service.Application("DC24 Ingester Platform")
service_facade = platform_service.makeService("sqlite:///ingester.db", {"db":"sqlite:///repo.db","files":"repo"})
service = internet.TCPServer(8080, webservice.makeResettableServer(service_facade))
service.setServiceParent(application)

ingester.startIngester(service_facade)


