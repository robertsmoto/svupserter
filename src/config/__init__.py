from os.path import exists
import json
import os

# LOAD THE ENVIRONMENT VARIABLES
def load_env(fpath: str) -> None:
    env_data = json.loads(open(fpath, "r").read())
    for key, value in env_data.items():
        os.environ[key] = value

confPath = "/etc/svupserter/conf.json"
file_exists = exists(confPath)
if not file_exists:
    confPath = "./conf-default.json"
    file_exists = exists(confPath)
if not file_exists:
    raise Exception("No config file. See the config/README.txt file.")
load_env(confPath)
