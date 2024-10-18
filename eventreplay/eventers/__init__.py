import importlib as _importlib

def client(name, **kwargs):
    """
    Create client
    """
    module = _importlib.import_module(f'{__name__}.{name}')
    kwargs.pop('eventer', None)
    return module.client(**kwargs)
