import logging

def init(level="DEBUG"):
	logging.addLevelName( logging.INFO, "\033[0;m%s\033[0;m" % logging.getLevelName(logging.INFO))
	logging.addLevelName( logging.DEBUG, "\033[0;32m%s\033[0;32m" % logging.getLevelName(logging.DEBUG))
	logging.addLevelName( logging.WARNING, "\033[0;33m%s\033[0;33m" % logging.getLevelName(logging.WARNING))
	logging.addLevelName( logging.ERROR, "\033[0;31m%s\033[0;31m" % logging.getLevelName(logging.ERROR))
	logging.addLevelName( logging.CRITICAL, "\033[0;35m%s\033[0;35m" % logging.getLevelName(logging.CRITICAL))

	logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s\033[0m', level=level)

def shutdown():
	logging.shutdown()
