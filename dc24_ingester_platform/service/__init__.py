"""This package contains all the service modules. These will be presented as a service
facade, to aggregate all the operations into transactionally safe operations.
"""

class ServiceFacade(object):
    def __init__(self, ingester, repository):
        self.ingester = ingester
        self.repository = repository


class IRepositoryService(object):
    """Interface for data management service
    """
    pass


class IIngesterService(object):
    """Interface for ingester service
    """
    def persistDataset(self, dataset):
        raise NotImplementedError()
    def deleteDataset(self, dataset):
        raise NotImplementedError()
    def getDataset(self, id=None):
        raise NotImplementedError()
    def getActiveDatasets(self):
        raise NotImplementedError()

def makeService(db_url, repo_url):
    """Construct a service facade from the provided service URLs
    
    If the repo_url is a DAM url construct a DAM repo. If the repo_url is a dict
    then construct a simple local repository
    """
    import ingesterdb
    
    ingester = ingesterdb.IngesterServiceDB(db_url)
    repo = None
    return ServiceFacade(ingester, repo)