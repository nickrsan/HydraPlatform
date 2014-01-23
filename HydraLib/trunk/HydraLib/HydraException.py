class DBException(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)


class HydraError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)


class HydraPluginError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)

#
#ERROR CODES FOR HYDRA
#Categories are:
#Permissions Errors: 000 - 099
#Formatting Errors:  100 - 199
#File Errors:        200 - 299
#Template Errors:    300 - 399
#Plugin Errors       400 - 499
#
errors = {
	'HYD_NET_001'  : "Invalid network",
	'HYD_PERM_001' : "Permission Denied.",

}