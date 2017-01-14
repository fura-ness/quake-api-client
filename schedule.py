from exceptions import QuakeAPIException


def create_schedule(quake, query_id, action=None, email_notify_on_start=None, 
                    email_notify_on_error=None, email_notify_on_finish=None,
                    start_time=None, variables=None, deliveries=None):

    params = {'executed_by': quake.auth_username,
              'action': action,
              'query_id': query_id,
              'email_notify_on_start': email_notify_on_start,
              'email_notify_on_error': email_notify_on_error,
              'email_notify_on_finish': email_notify_on_finish,
              'start_time': start_time,
              'variables': variables,
              'deliveries': deliveries}

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/schedules'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def get_schedule(quake, schedule_id):

    url = '/api/schedules/%s' % schedule_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
            raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def update_schedule(quake, schedule_id, partition_count=1, email_notify_on_start=None,
                    email_notify_on_error=None, email_notify_on_finish=None,
                    start_time=None, variables=None, deliveries=None):

    params = {'executed_by': quake.auth_username,
              'email_notify_on_start': email_notify_on_start,
              'email_notify_on_error': email_notify_on_error,
              'email_notify_on_finish': email_notify_on_finish,
              'start_time': start_time,
              'variables': variables,
              'deliveries': deliveries}

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/schedules/%s' % schedule_id
    status_code, query_response = quake.put(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response



