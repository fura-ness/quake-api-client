from exceptions import QuakeClientException, QuakeAPIException
from wait import wait as wait_func


def create_delivery(quake, execution_id, delivery_method, delivery_target,
                    database_token=None, table_overwrite=None, email_format=None,
                    mysql_insert_option=None, s3_delivery_bucket=None,
                    redshift_distkey=None, redshift_sortkey=None,
                    use_bulk_copy=None, create_indexes=None, wait=False):

    params = {'execution_id': execution_id,
              'delivery_method': delivery_method,
              'delivery_target': delivery_target,
              'table_overwrite': table_overwrite,
              'email_format': email_format,
              'mysql_insert_option': mysql_insert_option,
              'delivery_database_token': database_token,
              'redshift_distkey': redshift_distkey,
              'redshift_sortkey': redshift_sortkey,
              's3_delivery_bucket': s3_delivery_bucket,
              'create_indexes': create_indexes,
              'use_bulk_copy': use_bulk_copy}

    params = {k:v for k,v in params.items() if v is not None}

    if delivery_method == 'database':
        if not database_token and not delivery_database_id:
            raise QuakeClientException, 'Database token must be passed if using a database delivery.'

    url = '/api/deliveries'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    if wait:
        query_response = wait_func(poll_action=lambda: get_delivery(quake, query_response['id']),
                                   poll_test=lambda r: r['status'] in ('error','finished'),
                                   exception_message="PyQuake waited too long for delivery %s of execution %s. "
                                                     "PyQuake did not cancel the delivery." % (query_response['id'], execution_id))


    return query_response


def get_delivery(quake, delivery_id):

    url = '/api/deliveries/%s' % delivery_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def retry_delivery(quake, delivery_id):

  params = {'action': 'clone'}

  url = '/api/deliveries/%s' % delivery_id
  status_code, query_response = quake.post(url, params=params)

  if status_code != 200:
    raise QuakeAPIException, 'api error: %s' % query_response

  return query_response

