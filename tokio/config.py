#!/usr/bin/env python
"""
Load the pytokio configuration file, which is encoded as json and contains
various site-specific constants, paths, and defaults.
"""

import os
import sys
import json

CONFIG = {}
"""Global variable for the parsed configuration
"""

PYTOKIO_CONFIG_FILE = ""
"""Path to configuration file to load
"""

_LOADED_CONFIG = {}
"""Global variable for the parsed configuration file; used for unit testing
"""

DEFAULT_CONFIG_FILE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'site.json')
"""Path of default site configuration file
"""

def init_config():
    global PYTOKIO_CONFIG_FILE
    global _LOADED_CONFIG
    global CONFIG
    global DEFAULT_CONFIG_FILE

    # Load a pytokio config from a special location
    PYTOKIO_CONFIG_FILE = os.environ.get('PYTOKIO_CONFIG', DEFAULT_CONFIG_FILE)

    try:
        with open(PYTOKIO_CONFIG_FILE, 'r') as config_file:
            _LOADED_CONFIG = json.load(config_file)
    except (OSError, IOError):
        _LOADED_CONFIG = {}

    # Load pytokio config file and convert its keys into a set of constants
    _LOADED_CONFIG = json.load(open(PYTOKIO_CONFIG_FILE, 'r'))
    for _key, _value in _LOADED_CONFIG.iteritems():
        # config keys beginning with an underscore get skipped
        if _key.startswith('_'):
            pass

        # if setting this key will overwrite something already in the tokio.config
        # namespace, only overwrite if the existing object is something we probably
        # loaded from a json
        _old_attribute = getattr(sys.modules[__name__], _key.upper(), None)
        if _old_attribute is None \
        or isinstance(_old_attribute, basestring) \
        or isinstance(_old_attribute, dict) \
        or isinstance(_old_attribute, list):
            setattr(sys.modules[__name__], _key.upper(), _value)

    # Check for magic environment variables to override the contents of the config
    # file at runtime
    for _magic_variable in ['HDF5_FILES', 'ISDCT_FILES', 'LFSSTATUS_FULLNESS_FILES', 'LFSSTATUS_MAP_FILES']:
        _magic_value = os.environ.get("PYTOKIO_" + _magic_variable)
        if _magic_value is not None:
            try:
                _magic_value_decoded = json.loads(_magic_value)
            except ValueError:
                setattr(sys.modules[__name__], _magic_variable, _magic_value)
            else:
                setattr(sys.modules[__name__], _magic_variable, _magic_value_decoded)

init_config()
