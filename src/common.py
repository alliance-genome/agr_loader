import logging
import os
from typing import Dict

import yaml

logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if (cls, args, frozenset(kwargs.items())) not in cls._instances:
            cls._instances[(cls, args, frozenset(kwargs.items()))] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[(cls, args, frozenset(kwargs.items()))]


class NoDefaultValueError(Exception):
    pass


class ContextInfo(metaclass=Singleton):
    def __init__(self):
        # set default context info here
        config_file = open('src/default_env_vars.yml')
        self.env = yaml.load(config_file, Loader=yaml.FullLoader)

        # Look for ENV variables to replace default variables from config file.
        for key in self.env.keys():
            try:
                self.env[key] = ContextInfo._parse_environ_var(os.environ[key])
            except KeyError:
                # If we don't find an ENV variable, keep the value from the config file if is not none. Raise exception
                # otherwise
                if self.env[key] != "none":
                    logger.warning("variable " + key + " not set. Using default value " + str(self.env[key]))
                else:
                    logger.error("required variable " + key + " not set and no default value provided")
                    raise NoDefaultValueError

    @staticmethod
    def _parse_environ_var(env_var_value):
        if env_var_value == "true" or env_var_value == "True":
            return True
        elif env_var_value == "false" or env_var_value == "False":
            return False
        else:
            return env_var_value



