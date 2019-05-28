import configparser

cfg = configparser.ConfigParser()
cfg.read('settings.ini')
Config = cfg._sections
