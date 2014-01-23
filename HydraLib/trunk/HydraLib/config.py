import os
import glob
import ConfigParser

global CONFIG
CONFIG = None

def load_config():
    """Load a config file. This function looks for a config (*.ini) file in the
    following order::

        (1) ./*.ini
        (2) ~/.config/hydra/
        (3) /etc/hydra
        (4) [...]/HYDRA/HydraLib/trunk/../../config/

    (1) will override (2) will override (3) will override (4). Parameters not
    defined in (1) will be taken from (2). Parameters not defined in (2) will
    be taken from (3).  (3) is the config folder that will be checked out from
    the svn repository.  (2) Will be be provided as soon as an installable
    distribution is available. (1) will usually be written individually by
    every user."""

    global CONFIG

    #TODO: Check for the operating system we are running, provide search paths
    #      for Windows machines.
    modulepath = os.path.dirname(os.path.abspath(__file__))

    localfiles = glob.glob('*.ini')
    userfiles = glob.glob(os.path.expanduser('~') + '/.config/hydra/*.ini')
    sysfiles = glob.glob('/etc/hydra/*.ini')
    repofiles = glob.glob(modulepath + '/../../../config/*.ini')

    defaults={}
    defaults['home_dir'] = os.path.expanduser(os.environ.get('HYDRA_HOME_DIR', '~'))
    defaults['hydra_base_dir'] = os.path.expanduser(os.environ.get('HYDRA_BASE_DIR', '~/svn/HYDRA'))

    config = ConfigParser.ConfigParser(defaults, allow_no_value=True)

    for ini_file in repofiles:
        config.read(ini_file)
    for ini_file in sysfiles:
        config.read(ini_file)
    for ini_file in userfiles:
        config.read(ini_file)
    for ini_file in localfiles:
        config.read(ini_file)
    
    CONFIG = config

    return config

def get(section, option):

    if CONFIG is None:
        load_config()

    return CONFIG.get(section, option)

def getint(section, option):

    if CONFIG is None:
        load_config()

    return CONFIG.getint(section, option) 
