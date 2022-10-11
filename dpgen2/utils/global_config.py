from copy import deepcopy

CONFIG = {}

def store_global_config(config):
    CONFIG['config'] = deepcopy(config)

def get_global_config(key):
    return deepcopy(CONFIG['config'][key])
    