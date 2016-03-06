import sys
import config
import requests

def get_param_values(param):
    response = requests.get('{}get_param_values/?key={}&param={}'.format(config.BASE_URL, config.API_KEY, param))

    print response.status_code
    print response.text

def get_values(start_date, end_date):
    response = requests.get('{}api_GET/?key={}&agg_level_desc=COUNTY&format=JSON&year_GE={}&year_LT={}'.format(config.BASE_URL, config.API_KEY, start_date.split('-')[0], end_date.split('-')[0]))

    print response.status_code
    print response.text