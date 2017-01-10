class QuakeAPIException(Exception):
    pass

class QuakeClientException(Exception):
    pass

class DuplicateQueryException(Exception):
    existing_query = None
    pass

class QuakeAuthException(Exception):
    pass
