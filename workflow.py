from wait import wait as wait_func
from exceptions import QuakeAPIException


def create_workflow_schedule(quake, steps, run_times, active, name, description=None):

    params = {'created_by': quake.auth_username,
              'steps': steps,
              'run_times': run_times,
              'active': active,
              'name': name,
              'description': description}

    params = {k:v for k,v in params.items() if v}
    url = '/api/workflow-schedules'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def udpate_workflow_schedule(quake, workflow_schedule_id, steps, run_times, active, name, description=None):

    params = {'created_by': quake.auth_username,
              'steps': steps,
              'run_times': run_times,
              'active': active,
              'name': name,
              'description': description}

    params = {k:v for k,v in params.items() if v is not None}

    url = '/api/workflow-schedules/%s' % workflow_schedule_id
    status_code, query_response = quake.put(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def run_workflow_schedule_and_wait(quake, workflow_schedule_id):
    return run_workflow_schedule(quake, workflow_schedule_id, wait=True)


def run_workflow_schedule(quake, workflow_schedule_id, wait=False):

    params = {'action': 'run'}

    url = '/api/workflow-schedules/%s' % workflow_schedule_id
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    if wait:
        query_response = wait_func(poll_action=lambda: get_workflow(quake, query_response['id']),
                                   poll_test=lambda r: r['status'] in ('error','canceled','finished'),
                                   exception_message="PyQuake waited too long for workflow %s. "
                                                     "PyQuake did not cancel the workflow." % query_response['id'])



    return query_response

def clone_workflow_schedule(quake, workflow_schedule_id):

	params = {'action': 'clone'}

	url = '/api/workflow-schedules/%s' % workflow_schedule_id
	status_code, query_response = quake.post(url, params=params)

	if status_code != 200:
		raise QuakeAPIException, 'api error: %s' % query_response

	return query_response

def get_workflow_schedule(quake, workflow_schedule_id):

    url = '/api/workflow-schedules/%s' % workflow_schedule_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response

def get_workflow(quake, workflow_id):

    url = '/api/workflow/%s' % workflow_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response

