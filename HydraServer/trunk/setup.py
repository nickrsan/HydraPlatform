import sys
from cx_Freeze import setup, Executable

includes = ["encodings.utf_8",\
            "encodings.ascii",\
            "lxml._elementpath",\
            "ConfigParser",\
            "mysql.connector",\
            "suds",\
	    "pytz"]

setup(
    name = "On Dijkstra's Algorithm",
    version = "3.1",
    description = "A Dijkstra's Algorithm help tool.",
    executables = [Executable("server.py"), Executable("test.py")],
    options     = {
                        "build_exe": 
                        {
                            "includes": includes,
                        }
                }
)
