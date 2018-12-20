class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if (cls, args, frozenset(kwargs.items())) not in cls._instances:
            cls._instances[(cls, args, frozenset(kwargs.items()))] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[(cls, args, frozenset(kwargs.items()))]


class ContextInfo(metaclass=Singleton):
    def __init__(self):
        self.config_file_location = None
        self.verbose = None
