class QuakeAPIException(Exception):
    pass

class QuakeClientException(Exception):
    pass

class DuplicateQueryException(Exception):
    existing_query = None
    pass

class QuakeAuthException(Exception):
    pass

class QueryNotFoundException(Exception):
    pass

class QuakeWaitTimeoutException(Exception):
    pass

class QuakePollException(Exception):
    pass

class QuakePollTestException(Exception):
    pass

class QuakeStatusException(Exception):
	pass
