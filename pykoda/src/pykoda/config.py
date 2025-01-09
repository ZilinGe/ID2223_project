import os
import warnings
import configparser
import appdirs

# 获取调用脚本的目录
CALLER_DIR = os.getcwd()  # 当前工作目录
CONFIG_FILE = os.path.join(CALLER_DIR, 'config.ini')

if os.path.exists(CONFIG_FILE):
    print(f"Debug: 找到配置文件 {CONFIG_FILE}，开始读取配置文件内容。")
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    config_data = parser['all']

    # 打印 config.ini 文件中的所有内容
    for key, value in config_data.items():
        print(f"Debug: {key} = {value}")
else:
    print(f"Debug: 未找到配置文件 {CONFIG_FILE}，使用默认配置。")
    config_data = dict()

# 创建 'Cache' 文件夹
CACHE_DIR = os.path.join(CALLER_DIR, 'Cache')
os.makedirs(CACHE_DIR, exist_ok=True)
print(f"Debug: 创建缓存目录 {CACHE_DIR}（如果不存在）。")

# 读取 CPU 配置
N_CPU = int(config_data.get('n_cpu', -1))
print(f"Debug: 读取到的 CPU 配置为 N_CPU = {N_CPU}")

# 读取 API_KEY 配置
API_KEY = config_data.get('api_key', '')
if API_KEY:
    print(f"Debug: 读取到的 API_KEY = {API_KEY}")
else:
    print(f"Debug: 未在配置文件中找到 API_KEY。")

# 处理 API 版本逻辑
if not API_KEY:
    _msg = f'Config file {CONFIG_FILE} is missing the api key, please specify the parameter "api_key". ' \
           'Falling back to v1 of the API for download.'
    warnings.warn(RuntimeWarning(_msg))
    API_VERSION = 1
else:
    API_VERSION = 2
print(f"Debug: 使用 API 版本 {API_VERSION}")
