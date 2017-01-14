import sys

DEBUG = False

API_HOST = 'localhost'
API_HOST_PORT = 80
API_PROTOCOL = 'http'

DEFAULT_AUTH = ('user', 'pass')

VERSION = '1.31'
USER_AGENT = 'pyquake %s - python %s' % (VERSION, sys.version.replace('\n', ' ').replace('  ', ' '))
