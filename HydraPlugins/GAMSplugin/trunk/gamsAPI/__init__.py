import sys
import os

if sys.platform == 'linux2' and 'x86_64' in os.uname():
    from lib_linux_x86_64_27 import *
elif sys.platform == 'linux2' and 'i686' in os.uname():
    from lib_linux_i686_27 import *
elif sys.platform == 'win32' or sys.platform == 'cygwin':
    from lib_win32_27 import *
else:
    raise ImportError("GAMS API for platform '%s' not found." % sys.platform)
