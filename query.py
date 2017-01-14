import urllib

from exceptions import QuakeAPIException, QuakeClientException, QueryNotFoundException, DuplicateQueryException


def get_query(quake, query_id):

    assert isinstance(query_id, (int,long)), "Argument to get_query must be an integer"

    url = '/api/queries/%d' % query_id
    status_code, query_response = quake.get(url)

    if status_code == 404:
        raise QueryNotFoundException, "Query %s not found" % query_id

    elif status_code != 200:
        raise QuakeAPIException, '%s api error: %s' % (status_code, query_response)

    return query_response


def search_queries(quake, **kwargs):

    if not kwargs:
        raise QuakeAPIException, 'Search parameters are required.'

    url_params = urllib.urlencode(kwargs)

    url = '/api/queries?%s' % url_params
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, '%s api error: %s' % (status_code, query_response)

    return query_response


def get_or_create_query(quake, sql, database_id=None, database_token=None, title=None, description=None):

    try:
        query = create_query(quake, sql, database_id=database_id, database_token=database_token, title=title, description=description)
        return query, True
    except DuplicateQueryException as e:
        return e.existing_query, False


def create_query(quake, sql, database_id=None, database_token=None, title=None, description=None):

    if not isinstance(sql, basestring):
        raise QuakeClientException, "First parameter to get_or_create_query must be a string"

    params = {'created_by': quake.auth_username,
              'original_sql': sql,
              'title': title,
              'description': description}

    if database_id is not None:
        params['database_id'] = database_id
    elif database_token is not None:
        params['database_token'] = database_token

    params = {k:v for k,v in params.items() if v}

    url = '/api/queries'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:

        duplicate = query_response.get('_meta', {}).get('type') == 'IntegrityError'

        if duplicate:
            existing_query_id = query_response.get('_meta', {}).get('existing_query', {}).get('id')
            e = DuplicateQueryException('This is a duplicate of query id %s.' % existing_query_id)
            e.existing_query = query_response.get('_meta', {}).get('existing_query')
            raise e

        raise QuakeAPIException, '%s api error: %s' % (status_code, query_response)

    return query_response


def delete_query(quake, query_id):

    params = {'created_by': quake.auth_username}
    url = '/api/queries/%s' % query_id
    status_code, query_response = quake.delete(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def update_query(quake, query_id, sql=None, title=None, description=None, archived=None):

    if not isinstance(query_id, (int,long)):
        raise QuakeClientException, "First parameter to update_query must be an integer"

    params = {
        'created_by': quake.auth_username,
        'original_sql': sql,
        'title': title,
        'description': description,
        'archived': archived
    }

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/queries/%d' % query_id
    status_code, query_response = quake.put(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, '%s api error: %s' % (status_code, query_response)

    return query_response


def clone_query(quake, query_id, database_id=None, database_token=None, sql=None, title=None, description=None):

    params = {
        'query_id': query_id,
        'database_id': database_id,
        'database_token': database_token,
        'original_sql': sql,
        'title': title,
        'description': description,
        'action': 'clone'
    }

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/queries/%d' % query_id
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, '%s api error: %s' % (status_code, query_response)

    return query_response
