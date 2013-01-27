

class IngesterError(Exception):
    """Generic persistence exception"""
    def __init__(self, msg):
        Exception.__init__(self, msg)
   