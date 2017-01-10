from exceptions import *

def create_execution(quake, query_id, partition_count=1,
                     email_notify_on_start=None, email_notify_on_error=None,
                     email_notify_on_finish=None, start_time=None):

    params = {'executed_by': quake.auth_username,
              'action': 'queue' if (start_time is None) else 'schedule',
              'query_id': query_id,
              'partition_count': partition_count,
              'email_notify_on_start': email_notify_on_start,
              'email_notify_on_error': email_notify_on_error,
              'email_notify_on_finish': email_notify_on_finish,
              'start_time': start_time}

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/executions'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def get_execution(quake, execution_id):

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def get_results(quake, execution_id):

    if not isinstance(execution_id, (int, long)):
        raise QuakeClientException, 'Bad execution_id argument: %s' % execution_id

    url = '/api/executions/%s/results' % execution_id

    for line in quake.stream(url):
        yield line


def cancel_execution(quake, execution_id):

    params = {'action': 'cancel'}

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def delete_execution(quake, execution_id):

    params = {}

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.delete(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response
