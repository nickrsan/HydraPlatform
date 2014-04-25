# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
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
import os
from HydraLib.HydraException import HydraError
from HydraLib import config
import base64
import logging
log = logging.getLogger(__name__)

def add_image(name, file,**kwargs):
    path = config.get('filesys', 'img_src')
    try:
        os.makedirs(path)
    except OSError:
        pass

    path = os.path.join(path, name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path)
        raise HydraError("A file with this name (%s) already exists!"%(name))
    except IOError:
        pass


    log.info("Path: %r" % path)
    if not path.startswith(path):
        log.critical("Could not open file: %s"%name)
        return False

    f = open(path, 'w') # if this fails, the client will see an
    # # internal error.

    try:
        for data in file:
            f.write(data)

        log.debug("File written: %r" % name)

        f.close()

    except:
        log.critical("Error writing to file: %s", name)
        f.close()
        os.remove(name)
        log.debug("File removed: %r" % name)
        return False

    return True

def get_image(name,**kwargs):
    path = config.get('filesys', 'img_src')

    path = os.path.join(path, name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path, 'rb')
    except IOError:
        raise HydraError("File with name (%s) does not exist!"%(name))

    #read the contents of the file
    imageFile = f.read()

    #encode the contents of the file as a byte array
    encodedFile = base64.b64encode(imageFile)

    return encodedFile

def remove_image(name,**kwargs):
    path = config.get('filesys', 'img_src')

    path = os.path.join(path, name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path)
    except IOError:
        raise HydraError("File with name (%s) does not exist!"%(name))

    os.remove(path)

    return True


def add_file(resource_type, resource_id, name, file,**kwargs):
    path = config.get('filesys', 'file_src')
    path = os.path.join(path, resource_type)
    try:
        os.makedirs(path)
    except OSError:
        pass
    
    path = os.path.join(path, str(resource_id))
    try:
        os.makedirs(path)
    except OSError:
        pass

    path = os.path.join(path, name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path)
        raise HydraError("A file with this name (%s) already exists!"%(name))
    except IOError:
        pass


    log.info("Path: %r" % path)
    if not path.startswith(path):
        log.critical("Could not open file: %s"%name)
        return False

    f = open(path, 'w') # if this fails, the client will see an
    # # internal error.

    try:
        for data in file:
            f.write(data)

        log.debug("File written: %r" % name)

        f.close()

    except:
        log.critical("Error writing to file: %s", name)
        f.close()
        os.remove(name)
        log.debug("File removed: %r" % name)
        return False

    return True

def get_file(resource_type, resource_id, name,**kwargs):
    path = config.get('filesys', 'file_src')

    path = os.path.join(path, resource_type, str(resource_id), name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path, 'rb')
    except IOError:
        raise HydraError("File with name (%s) does not exist!"%(name))

    #read the contents of the file
    file_to_send = f.read()

    #encode the contents of the file as a byte array
    encodedFile = base64.b64encode(file_to_send)

    return encodedFile

def remove_file(resource_type, resource_id, name,**kwargs):
    path = config.get('filesys', 'file_src')

    path = os.path.join(path, resource_type, str(resource_id), name)

    #The safest way to check if a file exists is to try to open
    #it. If the open succeeds, then throw an exception to this effect.
    try:
        f = open(path)
    except IOError:
        raise HydraError("File with name (%s) does not exist!"%(name))

    os.remove(path)

    return True

