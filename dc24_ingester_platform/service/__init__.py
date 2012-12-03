"""This package contains all the service modules. These will be presented as a service
facade, to aggregate all the operations into transactionally safe operations.
"""

class IRepositoryService(object):
    """Interface for data management service
    """
    def persistObservation(self, dataset, schema, time, obs, cwd):
        """Persist the observation into the repository.

        :param dataset: the dataset object which the observation will be persisted in
        :param schema: the schema object for the dataset
        :param time: datetime for when the observation occurred
        :param observation: observation object
        :param cwd: current working directory which all files stored in the observation are relative to
        """
        raise NotImplementedError()

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
    def persisteSamplerState(self, dataset_id, state):
        raise NotImplementedError()
    def getSamplerState(self, dataset_id):
        raise NotImplementedError()
    def persisteDataSourceState(self, dataset_id, state):
        raise NotImplementedError()
    def getDataSourceState(self, dataset_id):
        raise NotImplementedError()
    def logIngesterEvent(self, dataset_id, timestamp, level, message):
        raise NotImplementedError()
    def getIngesterEvents(self, dataset_id):
        raise NotImplementedError()
    def findDatasets(self, **kwargs):
        raise NotImplementedError()
    def persistObservation(self, dataset, time, obs, cwd):
        raise NotImplementedError()

def method(verb, cls):
    """Annotation for identifying which class methods are responsible
    for different actions and classes
    :param verb: Action (persist, get, delete)
    :param cls: The serialised class name string
    """ 
    def _method(fn):
        fn.verb = verb
        fn.cls = cls
        return fn
    return _method

def find_method(self, verb, cls):
    for fn in dir(self):
        fn = getattr(self, fn)
        if hasattr(fn, "verb") and hasattr(fn, "cls") and fn.verb == verb and fn.cls == cls:
            return fn
    return None

def makeService(db_url, repo_url):
    """Construct a service facade from the provided service URLs
    
    If the repo_url is a DAM url construct a DAM repo. If the repo_url is a dict
    then construct a simple local repository
    """
    import ingesterdb
    
    if isinstance(repo_url, dict):
        import repodb
        repo = repodb.RepositoryDB(repo_url)
    else:
        import repodam
        repo = repodam.RepositoryDAM(repo_url)
    ingester_service = ingesterdb.IngesterServiceDB(db_url, repo=repo)
    return ingester_service
