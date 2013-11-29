import logging
from spyne.model.primitive import String, Boolean, Integer
from spyne.model.binary import ByteArray
from spyne.decorator import rpc
from hydra_base import HydraService
import os
from HydraLib.HydraException import HydraError
from HydraLib import config
import base64

class ImageService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(String, ByteArray, _returns=Boolean)
    def add_image(ctx, name, file):
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


        logging.info("Path: %r" % path)
        if not path.startswith(path):
            logging.critical("Could not open file: %s"%name)
            return False

        f = open(path, 'w') # if this fails, the client will see an
        # # internal error.

        try:
            for data in file:
                f.write(data)

            logging.debug("File written: %r" % name)

            f.close()

        except:
            logging.critical("Error writing to file: %s", name)
            f.close()
            os.remove(name)
            logging.debug("File removed: %r" % name)
            return False

        return True

    @rpc(String, _returns=ByteArray)
    def get_image(ctx, name):
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

    @rpc(String, _returns=Boolean)
    def remove_image(ctx, name):
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


class FileService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(String, Integer, String, ByteArray, _returns=Boolean)
    def add_file(ctx, resource_type, resource_id, name, file):
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


        logging.info("Path: %r" % path)
        if not path.startswith(path):
            logging.critical("Could not open file: %s"%name)
            return False

        f = open(path, 'w') # if this fails, the client will see an
        # # internal error.

        try:
            for data in file:
                f.write(data)

            logging.debug("File written: %r" % name)

            f.close()

        except:
            logging.critical("Error writing to file: %s", name)
            f.close()
            os.remove(name)
            logging.debug("File removed: %r" % name)
            return False

        return True

    @rpc(String,Integer,String, _returns=ByteArray)
    def get_file(ctx, resource_type, resource_id, name):
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

    @rpc(String,Integer, String, _returns=Boolean)
    def remove_file(ctx, resource_type, resource_id, name):
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

