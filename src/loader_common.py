"""This is mainly used to create contextInfo"""

import logging
import os
import yaml


class Singleton(type):
    """Singleton"""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if (cls, args, frozenset(kwargs.items())) not in cls._instances:
            cls._instances[(cls, args, frozenset(kwargs.items()))] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[(cls, args, frozenset(kwargs.items()))]


class NoDefaultValueError(Exception):
    """No Defualt Value Error"""


class ContextInfo(metaclass=Singleton):
    """Gets Configuration information from files and from ENV variables"""

    logger = logging.getLogger(__name__)

    def __init__(self, logger=logger):
        # set default context info here
        config_file = open('src/default_env_vars.yml')
        self.env = yaml.load(config_file, Loader=yaml.FullLoader)
        logger.info('Loading ContextInfo variables.')

        # Look for ENV variables to replace default variables from config file.
        for key in self.env.keys():
            try:
                self.env[key] = self._parse_environ_var(os.environ[key])
            except KeyError:
                # If we don't find an ENV variable,
                # keep the value from the config file if is not none.
                # Raise exception otherwise
                if self.env[key] != "none":
                    logger.warning("variable %s not set. Using default value %s",
                                   key,
                                   str(self.env[key]))
                else:
                    logger.error("required variable %s not set and no default value provided", key)
                    raise NoDefaultValueError
            logger.info('{} -> {}'.format(key, self.env[key]))

    @staticmethod
    def _parse_environ_var(env_var_value):
        """Determines if ENV variable is true or not"""

        return_value = env_var_value
        if env_var_value in ["true", "True"]:
            return_value = True
        elif env_var_value in ["false", "False"]:
            return_value = False

        return return_value
