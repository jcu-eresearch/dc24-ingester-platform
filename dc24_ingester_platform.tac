# Twistd TAC to start management service on port 8080
import os
import sys
import logging
import logging.config
import tempfile

from twisted.application import service, internet
from twisted.web import static, server
from twisted.web.resource import IResource, Resource
from twisted.python import log

# Setup logging before anything else gets imported
logging.config.fileConfig("logging.conf")

observer = log.PythonLoggingObserver()
observer.start()

from dc24_ingester_platform import webservice
from dc24_ingester_platform import ingester
from dc24_ingester_platform import push

from dc24_ingester_platform import service as platform_service

# attach the service to its parent application
application = service.Application("DC24 Ingester Platform")
service_facade = platform_service.makeService("sqlite:///ingester.db", {"db":"sqlite:///repo.db","files":"repo"})
root = Resource()
root.putChild("api", webservice.makeServer(tempfile.mkdtemp(), service_facade))
root.putChild("push", push.makePushService(service_facade, os.path.join(tempfile.gettempdir(), "push")))

from zope.interface import Interface, implements

from twisted.cred import portal
from twisted.cred import checkers

from twisted.web.guard import HTTPAuthSessionWrapper, DigestCredentialFactory, BasicCredentialFactory

class HTTPAuthRealm(object):
    implements(portal.IRealm)

    def __init__(self, resource):
        self.resource = resource

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IResource in interfaces:
            return IResource, self.resource, lambda: None

        raise NotImplementedError("Interface not supported: %s"%(str(interfaces)))

portal = portal.Portal(HTTPAuthRealm(root))
checker = checkers.InMemoryUsernamePasswordDatabaseDontUse(casey='password')
portal.registerChecker(checker)
credFactories = (BasicCredentialFactory('My Realm'),
    DigestCredentialFactory('md5', 'My Realm'))
root = HTTPAuthSessionWrapper(portal, credFactories)

service = internet.TCPServer(8080, server.Site(root))
service.setServiceParent(application)

ingester.start_ingester(service_facade, tempfile.gettempdir())


