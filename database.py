from exceptions import QuakeAPIException


def get_databases(quake):

    url = '/api/databases'
    status_code, databases = quake.get(url)

    if status_code != 200:
        raise QuakeAPIException, databases.get('_meta', {}).get('error', '')
    else:
        return databases['databases']
