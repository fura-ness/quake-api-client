import requests
import simplejson as json

from settings import *
from exceptions import *
import database
import query
import execution
import delivery

class Quake(object):

    def __init__(self, *args, **kwargs):

        self.api_host = kwargs.get('api_host') or API_HOST
        self.api_host_port = kwargs.get('api_host_port') or API_HOST_PORT
        self.api_protocol = kwargs.get('api_protocol') or API_PROTOCOL
        self.base_api_url = '%s://%s:%s' % (self.api_protocol, self.api_host, self.api_host_port)

        auth_username = kwargs.get('username')
        auth_password = kwargs.get('password')

        if DEBUG:
            self.auth_username = auth_username or DEFAULT_AUTH[0]
            self.auth_password = auth_password or DEFAULT_AUTH[1]
        else:
            assert auth_username and auth_password, 'Must provide username and password keyword arguments to Quake class constructor'
            self.auth_username = auth_username
            self.auth_password = auth_password

        self.auth = (self.auth_username, self.auth_password)


    def stream(self, url, params=None, method=requests.get):

        assert url.startswith('http') or url.startswith('/'), 'URL must begin with the server/protocol (http or https) or exclude the server and begin with /'
        request_url = url if url.startswith('http') else '%s%s' % (self.base_api_url, url)

        if params is not None:
            response = method(request_url, auth=self.auth, data=json.dumps(params), stream=True)
        else:
            response = method(request_url, auth=self.auth, stream=True)

        if response.status_code == 401:
            raise QuakeAuthException, response_dict.get('_meta', {}).get('description', '')

        for line in response.iter_lines():
            yield line


    def get(self, url, params=None):
        return self.call(requests.get, url, params=params)

    def post(self, url, params=None):
        return self.call(requests.post, url, params=params)

    def put(self, url, params=None):
        return self.call(requests.put, url, params=params)

    def delete(self, url, params=None):
        return self.call(requests.delete, url, params=params)

    def call(self, method, url, params=None):

        assert url.startswith('http') or url.startswith('/'), 'URL must begin with the server/protocol (http or https) or exclude the server and begin with /'
        request_url = url if url.startswith('http') else '%s%s' % (self.base_api_url, url)

        try:
            if params is not None:
                response = method(request_url, auth=self.auth, data=json.dumps(params))
            else:
                response = method(request_url, auth=self.auth)
        except requests.exceptions.ConnectionError as e:
            return 500, {'_meta': {'error': str(e)}}

        if response.status_code == 401:
            raise QuakeAuthException, response_dict.get('_meta', {}).get('description', '')

        try:
            response_dict = response.json()
        except:
            response_dict = {'_meta': {'error': response.text}}

        # deal with inconsistent locations of error message
        if '_meta' in response_dict:
            if not response_dict['_meta'].get('error'):
                response_dict['_meta']['error'] = response_dict['_meta'].get('description', '')

        return response.status_code, response_dict

    def __getattr__(self, name):

        for module in database, query, execution, delivery:
            if hasattr(module, name):
                func = getattr(module, name)
                if callable(func):
                    def method(*args, **kwargs):
                        return func(self, *args, **kwargs)

                    return method

        raise AttributeError, 'Name "%s" not found' % name
