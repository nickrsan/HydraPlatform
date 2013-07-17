from db import HydraIface

if __name__ == '__main__':
	x = HydraIface.Project()
	x.load()
	x.update()
