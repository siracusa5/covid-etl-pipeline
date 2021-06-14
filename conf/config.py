from configparser import ConfigParser
import os

global config


class Config:
    conf = ConfigParser()
    conf.read(os.path.join(os.path.dirname(__file__), '..', 'conf/config.ini'))
    # conf = {s: dict(config.items(s)) for s in config.sections()}

    MYSQL_HOST = conf['mysql']['host']
    MYSQL_PORT = conf['mysql']['port']
    MYSQL_DB = conf['mysql']['database']
    MYSQL_USER = conf['mysql']['user']
    MYSQL_PW = conf['mysql']['password']

    API_BASE_URI = conf['covid_api']['base_uri']

    DATES = ['20200429', '20200430']

    STATES = [  # 'al',
        # 'ak',
        # 'as',
        # 'az',
        # 'ar',
        'ca',
        # 'co',
        # 'ct',
        # 'de',
        # 'dc',
        # 'fm',
        # 'fl',
        # 'ga',
        # 'gu',
        # 'hi',
        # 'id',
        # 'il',
        # 'in',
        # 'ia',
        # 'ks',
        # 'ky',
        # 'la',
        # 'me',
        # 'mh',
        # 'md',
        # 'ma',
        # 'mi',
        # 'mn',
        # 'ms',
        # 'mo',
        # 'mt',
        # 'ne',
        # 'nv',
        # 'nh',
        # 'nj',
        # 'nm',
        # 'ny',
        # 'nc',
        # 'nd',
        # 'mp',
        'oh',
        # 'ok',
        # 'or',
        # 'pw',
        # 'pa',
        # 'pr',
        # 'ri',
        # 'sc',
        # 'sd',
        # 'tn',
        'tx'
        # 'ut',
        # 'vt',
        # 'vi',
        # 'va',
        # 'wa',
        # 'wv',
        # 'wi',
        # 'wy'
    ]
