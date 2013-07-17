import util

global CNX
CNX = None

class IfaceBase(object):
	def __init__(self, table_name):
		global CNX
		if CNX is None:
			CNX = util.connect()
		self.db = IfaceDB(table_name)

	def load(self):
		pass

	def commit(self):
		pass

	def save(self):
		pass

class IfaceDB(object):

	def __init__(self, table_name):
		self.db_attrs = []
		self.pk_attrs = []
		self.nullable_attrs = []
		self.attr_types = {}

		self.table_name = table_name
		cursor = CNX.cursor()
		cursor.execute('desc %s' % table_name)

		table_desc = cursor.fetchall()

		print table_desc

		for col_desc in table_desc:
			col_name = col_desc[0]

			setattr(self, col_name, col_desc[4])
			self.db_attrs.append(col_name)

			self.attr_types[col_name] = col_desc[1]

			if col_desc[2] == 'YES':
				self.nullable_attrs.append(col_name)

			if col_desc[3] == 'PRI':
				self.pk_attrs.append(col_name)

		self.update()
	
	def insert(self):
		#A function to return 'null' if the inputted value is None. Otherwise return the inputted value.
		v = lambda x: getattr(self, x) if getattr(self, x) is not None else 'null'

		base_insert = "insert into %(table)s %(cols)s values %(vals)s;"
		complete_insert = base_insert % dict(
			table = self.table_name,
			cols  = ",".join([n for n in self.db_attrs]),
			vals  = ",".join([v(n) for n in self.db_attrs]),
		)

		print complete_insert
		#CNX.cursor.execute(complete_insert)
	
	def update(self):
		#A function to return 'null' if the inputted value is None. Otherwise return the inputted value.
		v = lambda x: getattr(self, x) if getattr(self, x) is not None else 'null'

		base_update = "update %(table)s set %(sets)s where %(pk)s;"
		complete_update = base_update % dict(
			table = self.table_name,
			sets  = ",".join(["%s = %s"%(n, v(n)) for n in self.db_attrs]),
			pk    = ",".join(["%s = %s"%(n, v(n)) for n in self.pk_attrs]),
		)

		print complete_update
		#CNX.cursor.execute(complete_update)

class Project(IfaceBase):
	def __init__(self):
		IfaceBase.__init__(self, 't%s'%self.__class__.__name__)
		print dir(self.db)

	def insert(self):
		pass

	def update(self):
		pass

