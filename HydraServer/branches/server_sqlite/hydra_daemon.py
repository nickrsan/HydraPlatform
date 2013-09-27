import sys
import os
from daemon import Daemon

from server import HydraServer


class HydraDaemon(Daemon):

    def run(self):
        server = HydraServer()
        server.run_server()


if __name__ == '__main__':

    HOMEDIR = os.path.expanduser('~')
    pidfile = HOMEDIR + '/.hydra/hydra_server.pid'
    outfile = HOMEDIR + '/.hydra/hydra.out'
    errfile = HOMEDIR + '/.hydra/hydra.err'

    daemon = HydraDaemon(pidfile, stdout=outfile, stderr=errfile)

    if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                    daemon.start()
            elif 'stop' == sys.argv[1]:
                    daemon.stop()
            elif 'restart' == sys.argv[1]:
                    daemon.restart()
            else:
                    print "Unknown command"
                    sys.exit(2)
            sys.exit(0)
    else:
            print "usage: %s start|stop|restart" % sys.argv[0]
            sys.exit(2)
