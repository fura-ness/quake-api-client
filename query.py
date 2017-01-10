from exceptions import *


def get_or_create_query(quake, sql, database_id=None, database_token=None, title=None, description=None):

    try:
        query = create_query(quake, sql, database_id=database_id, database_token=database_token, title=title, description=description)
        return query, True
    except DuplicateQueryException as e:
        return e.existing_query, False


def create_query(quake, sql, database_id=None, database_token=None, title=None, description=None):

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
