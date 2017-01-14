from exceptions import QuakeClientException, QuakeAPIException
from wait import wait as wait_func


def create_execution_and_wait(quake, query_id, **kwargs):
    return create_execution(quake, query_id, wait=True, **kwargs)


def create_execution(quake, query_id, partition_count=1, email_notify_on_start=None,
                     email_notify_on_error=None, email_notify_on_finish=None,
                     start_time=None, variables=None, wait=False):

    params = {'executed_by': quake.auth_username,
              'action': 'queue' if (start_time is None) else 'schedule',
              'query_id': query_id,
              'partition_count': partition_count,
              'email_notify_on_start': email_notify_on_start,
              'email_notify_on_error': email_notify_on_error,
              'email_notify_on_finish': email_notify_on_finish,
              'start_time': start_time,
              'variables': variables}

      url = '/api/executions'


    params = {k:v for k,v in params.items() if v is not None}

    status_code, query_response = quake.post(url, params=params)
    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    if wait:
        query_response = wait_func(poll_action=lambda: get_execution(quake, query_response['id']),
                                   poll_test=lambda r: r['status'] in ('error','canceled','finished'),
                                   exception_message="PyQuake waited too long for execution %s of query %s. "
                                                     "PyQuake did not cancel the execution." % (query_response['id'], query_id))

    return query_response


def get_execution(quake, execution_id):

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def search_executions(quake, query_id=None, error_message=None, executed_by=None, partition_count=None, schedule_id=None, has_schedule=None, status=None):

    if query_id and not isinstance(query_id, (int, long)):
        raise QuakeClientException, 'Bad query_id argument: %s' % query_id

    if schedule_id and not isinstance(schedule_id, (int, long)):
        raise QuakeClientException, 'Bad schedule_id argument: %s' % schedule_id

    params = {
        'query_id': query_id,
        'schedule_id': schedule_id,
        'error_message': error_message,
        'executed_by': executed_by,
        'partition_count': partition_count,
        'has_schedule': has_schedule,
        'status': status}

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/executions'
    status_code, query_response = quake.get(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def get_results(quake, execution_id, compressed=False):

    if not isinstance(execution_id, (int, long)):
        raise QuakeClientException, 'Bad execution_id argument: %s' % execution_id

    if compressed:
        url = '/api/executions/%s/results?compressed=true' % execution_id
    else:
        url = '/api/executions/%s/results' % execution_id

    for chunk in quake.stream(url, compressed=compressed):
        yield chunk


def cancel_execution(quake, execution_id):

    params = {'action': 'cancel'}

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def delete_execution(quake, execution_id):

    params = {'executed_by': quake.auth_username}

    url = '/api/executions/%s' % execution_id
    status_code, query_response = quake.delete(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def delete_executions(quake, query_id, keep_most_recent_count=1):

    if not isinstance(query_id, (int, long)):
        raise QuakeClientException, 'Bad query_id argument: %s' % query_id

    if not isinstance(keep_most_recent_count, (int, long)) or keep_most_recent_count < 0:
        raise QuakeClientException, 'Bad keep_most_recent_count argument: %s' % keep_most_recent_count

    executions = get_executions(quake, query_id=query_id, executed_by=quake.auth_username, status='finished')

    executions = executions['executions']
    executions = executions[keep_most_recent_count:]

    for e in executions:
        delete_execution(quake, e['id'])

    return len(executions)


def retry_execution(quake, execution_id):

  params = {'action': 'clone'}

  url = '/api/executions/%s' % execution_id
  status_code, query_response = quake.post(url, params=params)

  if status_code != 200:
    raise QuakeAPIException, 'api error: %s' % query_response

  return query_response
