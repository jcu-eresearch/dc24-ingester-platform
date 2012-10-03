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

import dc24_ingester_platform.service
import dc24_ingester_platform.ingester

# attach the service to its parent application
application = service.Application("DC24 Ingester Platform")
service = internet.TCPServer(8080, dc24_ingester_platform.service.makeServer())
service.setServiceParent(application)

dc24_ingester_platform.ingester.startIngester()


