import os, os.path, socket, ConfigParser

try:
    import simplejson as json
except:
    import json

import requests

from settings import API_HOST, API_HOST_PORT, API_PROTOCOL, DEBUG, DEFAULT_AUTH, USER_AGENT
from exceptions import QuakeClientException, QuakeAuthException
import database
import query
import execution
import delivery
import schedule
import workflow

class Quake(object):

    def __init__(self, *args, **kwargs):

        self.api_host = kwargs.get('api_host') or API_HOST
        self.api_host_port = kwargs.get('api_host_port') or API_HOST_PORT
        self.api_protocol = kwargs.get('api_protocol') or API_PROTOCOL
        self.base_api_url = '%s://%s:%s' % (self.api_protocol, self.api_host, self.api_host_port)

        auth_username = kwargs.get('username')
        auth_password = kwargs.get('password')

        if not auth_username or not auth_password:
            try:
                config = ConfigParser.RawConfigParser()
                pyquakerc = os.environ.get('PYQUAKERC') or os.path.expanduser('~/.pyquakerc')

                # ConfigParser doesn't raise IOError for a missing file but we'd prefer that it did
                with open(pyquakerc):
                    pass

                config.read(pyquakerc)
                auth_username = config.get('auth', 'username')
                auth_password = config.get('auth', 'password')
            except IOError as e:
                raise QuakeClientException, 'error with config file %s: %s' % (pyquakerc, e)
            except Exception as e:
                pass

        if DEBUG:
            self.auth_username = auth_username or DEFAULT_AUTH[0]
            self.auth_password = auth_password or DEFAULT_AUTH[1]
        else:
            assert auth_username and auth_password, 'Must provide username and password keyword arguments to Quake class constructor or in .pyquakerc file.'
            self.auth_username = auth_username
            self.auth_password = auth_password

        self.auth = (self.auth_username, self.auth_password)

        self.custom_headers = {'User-Agent': USER_AGENT}


    def stream(self, url, params=None, method=requests.get, compressed=False):

        request_url = url if url.startswith('http') else '%s%s' % (self.base_api_url, url)

        if params is not None:
            response = method(request_url, auth=self.auth, data=json.dumps(params), stream=True, headers=self.custom_headers)
        else:
            response = method(request_url, auth=self.auth, stream=True, headers=self.custom_headers)

        if response.status_code == 401:
            raise QuakeAuthException, response_dict.get('_meta', {}).get('description', '')

        if not compressed:
            for line in response.iter_lines():
                yield line
        else:
            chunk_size = 1024 * 1024
            for chunk in response.iter_content(chunk_size=chunk_size):
                yield chunk


    def get(self, url, params=None):
        return self.call(requests.get, url, params=params)

    def post(self, url, params=None, files=None):
        return self.call(requests.post, url, params=params, files=files)

    def put(self, url, params=None):
        return self.call(requests.put, url, params=params)

    def delete(self, url, params=None):
        return self.call(requests.delete, url, params=params)

    def call(self, method, url, params=None, files=None):

        request_url = url if url.startswith('http') else '%s%s' % (self.base_api_url, url)

        try:
            if params is not None:
                if method == requests.get:
                    response = method(request_url, auth=self.auth, params=params, headers=self.custom_headers)
                else:
                    if files:
                        response = method(request_url, auth=self.auth, data=params, files=files, headers=self.custom_headers)
                    else:
                        response = method(request_url, auth=self.auth, data=json.dumps(params), headers=self.custom_headers)

            else:
                response = method(request_url, auth=self.auth, headers=self.custom_headers)
        except requests.exceptions.ConnectionError as e:
            return 500, {'_meta': {'error': str(e)}}

        try:
            response_dict = response.json()
        except:
            response_dict = {'_meta': {'error': response.text}}

        if response.status_code == 401:
            raise QuakeAuthException, response_dict.get('_meta', {}).get('description', '')

        return response.status_code, response_dict

    def __getattr__(self, name):

        for module in database, query, execution, delivery, schedule, workflow:
            if hasattr(module, name):
                func = getattr(module, name)
                if callable(func):
                    def method(*args, **kwargs):
                        return func(self, *args, **kwargs)

                    return method

        raise AttributeError, 'Name "%s" not found' % name
