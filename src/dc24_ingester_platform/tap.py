from twisted.application import internet
from twisted.web import server
from twisted.web.resource import Resource
from twisted.python import usage
 
class SimpleResource(Resource):
    isLeaf = True
    def render_GET(self, request):
        return "<h1>It Works!</h1>"
 
 
class Options(usage.Options):
    pass
 
 
def makeService(config):
    site = server.Site(SimpleResource())
    return internet.TCPServer(8080, site)

