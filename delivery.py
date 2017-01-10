from exceptions import *


def create_delivery(quake, execution_id, delivery_method, delivery_target, 
                    database_token=None, table_overwrite=None, email_format=None,
                    sample_rows_only=None, mysql_insert_option=None):

    params = {'execution_id': execution_id,
              'delivery_method': delivery_method,
              'delivery_target': delivery_target,
              'table_overwrite': table_overwrite,
              'email_format': email_format,
              'sample_rows_only': sample_rows_only,
              'mysql_insert_option': mysql_insert_option,
              'delivery_database_token': database_token}

    params = {k:v for k,v in params.items() if v is not None}

    if delivery_method == 'database':
        if not database_token:
            raise QuakeClientException, 'Database token must be passed if using a database delivery.'

    url = '/api/deliveries'
    status_code, query_response = quake.post(url, params=params)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response


def get_delivery(quake, delivery_id):

    url = '/api/deliveries/%s' % delivery_id
    status_code, query_response = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, 'api error: %s' % query_response

    return query_response
