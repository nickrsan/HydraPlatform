# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
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

    config = ConfigParser.ConfigParser(allow_no_value=True)

    for ini_file in repofiles:
        config.read(ini_file)
    for ini_file in sysfiles:
        config.read(ini_file)
    for ini_file in userfiles:
        config.read(ini_file)
    for ini_file in localfiles:
        config.read(ini_file)

    if os.name == 'nt':
        import winpaths
        common_app_data = winpaths.get_common_appdata()
        config.set('DEFAULT', 'common_app_data_folder', common_app_data)
    else:
        config.set('DEFAULT', 'common_app_data_folder', '')

    try:
        home_dir = config.get('DEFAULT', 'home_dir')
    except:
        home_dir = os.environ.get('HYDRA_HOME_DIR', '~')
    config.set('DEFAULT', 'home_dir', os.path.expanduser(home_dir))

    try:
        hydra_base = config.get('DEFAULT', 'hydra_base_dir')
    except:
        hydra_base = os.environ.get('HYDRA_BASE_DIR', '~/svn/HYDRA')
    config.set('DEFAULT', 'hydra_base_dir', os.path.expanduser(hydra_base))


    CONFIG = config

    return config

def get(section, option, default=None):

    if CONFIG is None:
        load_config()

    try:
        return CONFIG.get(section, option)
    except:
        return default

def getint(section, option, default=None):

    if CONFIG is None:
        load_config()

    try:
        return CONFIG.getint(section, option)
    except:
        return default
    
