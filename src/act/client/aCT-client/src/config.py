"""
A set of functions and variables for handling configuration.

Every program gets configuration in 4 steps:
    1. as parameters for program
    2. remaining not provided parameters are read from config file given
    as parameter
    3. remaining not provided parameters are read from default config file
    4. remaining not provided parameters are given hardcoded values

If parameter is not available in any of first 3 steps, it remains None and is
given some hardcoded value in 4th step.
"""

import ConfigParser
import StringIO
import os
import sys

# program parameters that are paths and might have to be expanded
# (~) as home folder
PATH_PARAMS = ('proxy', 'cert', 'key', 'cadir')

# defaults that are used if no parameters or config is available
DEFAULT_CONF = os.path.join(os.path.expanduser('~'), '.aCT-client', 'aCT-client.conf')
DEFAULTS = {
        'proxy':    '/tmp/x509up_u' + str(os.getuid()),
        'server':   'https://node1',
        'port':     443,
        'cert':     os.path.join(os.path.expanduser('~'), '.globus', 'usercert.pem'),
        'key':      os.path.join(os.path.expanduser('~'), '.globus', 'userkey.pem'),
        'cadir':    '/etc/grid-security/certificates'
}

def parse_conf_file(conf_file, conf_dict):
    """
    Parse configuration file into given dictionary.

    All dictionary keys with None values will be taken from config.
    If they are not present in config, they remain None.
    """
    # inserting dummy section, as config parser requires it
    conf_str = '[dummy]\n' + conf_file.read()
    conf_fp = StringIO.StringIO(conf_str)
    conf_parser = ConfigParser.RawConfigParser()
    conf_parser.readfp(conf_fp)
    config = dict(conf_parser.items('dummy'))

    for key, value in conf_dict.items():
        if value == None:
            conf_dict[key] = config.get(key, None)


def set_defaults(conf_dict):
    """Set all None parameters to defaults values in config dictionary."""
    for key, value in conf_dict.items():
        if value is None:
            conf_dict[key] = DEFAULTS.get(key, None)


def expand_paths(conf_dict):
    """Expand home directories (~) in path parameters of config dictionary."""
    for param in PATH_PARAMS:
        if param in conf_dict.keys():
            conf_dict[param] = os.path.expanduser(conf_dict[param])


def parse_param_conf(conf_name, conf_dict):
    """
    Parse config file to dictionary and exit on error.

    If there are any errors, user should be notified and fix the problem.
    """
    try:
        conf_file = open(conf_name, 'r')
    except Exception as e:
        print 'error: {}'.format(str(e))
        sys.exit(2)
    else:
        parse_conf_file(conf_file, conf_dict)
        conf_file.close()


def parse_default_conf(conf_dict):
    """
    Parse default config file to dictionary and don't exit on error.

    Since default config files are not mandatory, the problem with the
    existence of file can be ignored.
    """
    try:
        conf_file = open(DEFAULT_CONF, 'r')
    except Exception as e:
        # Program doesn't complain when default conf is missing because that
        # might be intent of a user.
        pass
    else:
        config.parse_conf_file(conf_file, conf_dict)
        conf_file.close()


def parse_non_param_conf(conf_dict, conf_arg):
    """Parse from config files and use defaults for any not given parameter."""
    # get remaining config from conf file parameter
    if None in conf_dict.values() and conf_arg:
        parse_param_conf(conf_arg, conf_dict)

    # get remaining config from default conf file
    if None in conf_dict.values():
        parse_default_conf(conf_dict)

    # use defaults for all remaining not given parameters
    set_defaults(conf_dict)

    # if any path has '~' character, expand it
    expand_paths(conf_dict)


