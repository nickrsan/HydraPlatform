#from soap_server.network import *
#from soap_server.project import *


def hello():
    return "Hello, world"

def echo(*args):
    return args

def average(*args):
    sum = 0
    for i in args: sum += i
    return sum / len(args)

from ZSI import dispatch
dispatch.AsCGI()
