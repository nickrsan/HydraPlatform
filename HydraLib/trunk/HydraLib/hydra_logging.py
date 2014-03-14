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
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
import logging
import config
import os

def init(level=None):
    if level is None:
        level = config.get('DEFAULT', 'log_level')
        
    if os.name == "nt":
        logging.addLevelName( logging.INFO, logging.getLevelName(logging.INFO))
        logging.addLevelName( logging.DEBUG, logging.getLevelName(logging.DEBUG))
        logging.addLevelName( logging.WARNING, logging.getLevelName(logging.WARNING))
        logging.addLevelName( logging.ERROR, logging.getLevelName(logging.ERROR))
        logging.addLevelName( logging.CRITICAL, logging.getLevelName(logging.CRITICAL))
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=level)
        return
    
    if level is None:
        level = config.get('DEFAULT', 'log_level')

    logging.addLevelName( logging.INFO, "\033[0;m%s\033[0;m" % logging.getLevelName(logging.INFO))
    logging.addLevelName( logging.DEBUG, "\033[0;32m%s\033[0;32m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName( logging.WARNING, "\033[0;33m%s\033[0;33m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName( logging.ERROR, "\033[0;31m%s\033[0;31m" % logging.getLevelName(logging.ERROR))
    logging.addLevelName( logging.CRITICAL, "\033[0;35m%s\033[0;35m" % logging.getLevelName(logging.CRITICAL))

    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s\033[0m', level=level)

def shutdown():
	logging.shutdown()
