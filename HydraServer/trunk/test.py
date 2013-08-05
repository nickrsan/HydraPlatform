from ZSI.client import Binding
fp = open('debug.out', 'a')
b = Binding(url='soap', tracefile=fp)
fp.close()
a = b.average(range(1,11))
assert a == 5
print b.hello()
