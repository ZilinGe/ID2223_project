import os
import warnings
import configparser

import appdirs

# CONFIG_DIR = appdirs.user_config_dir('pykoda')
# CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.ini')

# 获取调用脚本的目录
CALLER_DIR = os.getcwd()  # 当前工作目录
CONFIG_FILE = os.path.join(CALLER_DIR, 'config.ini')

if os.path.exists(CONFIG_FILE):
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    config_data = parser['all']
else:
    config_data = dict()

# CACHE_DIR = config_data.get('cache_dir', appdirs.user_cache_dir('pykoda'))
# os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_DIR = os.path.join(CALLER_DIR, 'Cache')
os.makedirs(CACHE_DIR, exist_ok=True)  # 创建 'Cache' 文件夹

N_CPU = int(config_data.get('n_cpu', -1))
API_KEY = config_data.get('api_key', '')
if not API_KEY:
    _msg = f'Config file {CONFIG_FILE} is missing the api key, please specify the parameter "api_key".' \
           'Falling back to v1 of the API for download.'
    warnings.warn(RuntimeWarning(_msg))
    API_VERSION = 1
else:
    API_VERSION = 2
