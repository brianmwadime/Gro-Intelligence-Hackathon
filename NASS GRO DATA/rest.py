import sys
import config
import requests

def get_param_values(param):
    response = requests.get('{}get_param_values/?key={}&param={}'.format(config.BASE_URL, config.API_KEY, param)) # .format() config.BASE_URL)


def get_values():