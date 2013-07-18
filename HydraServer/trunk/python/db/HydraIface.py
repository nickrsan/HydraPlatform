import util
import logging
from HydraLib.HydraException import DBException

global CNX
CNX = None
global cursor
cursor = None

class IfaceBase(object):
	def __init__(self, class_name):
		logging.info("Initialising")
		global CNX
		global cursor
		if CNX is None:
			CNX = util.connect()
			cursor = CNX.cursor()
		self.db = IfaceDB(class_name)

		self.in_db = False

	def load(self):
		self.db.load()

	def commit(self):
		CNX.commit()
	
	def delete(self):
		if self.in_db:
			self.db.delete()

	def save(self):

		if self.in_db is True:
			if self.db.has_changed is True:
				self.db.update()
			else:
				logging.debug("No changes to %s, not continuing", self.db.table_name)
		else:
			self.db.insert()
			self.in_db = True

class IfaceDB(object):

	def __init__(self, class_name):
		self.db_attrs = []
		self.pk_attrs = []
		self.nullable_attrs = []
		self.attr_types = {}

		#This indicates whether any values in this table have changed.
		self.has_changed = False

		#this turns 'Project' into 'tProject' so it can identify the table
		self.table_name = "t%s"%class_name

		cursor.execute('desc %s' % self.table_name)

		table_desc = cursor.fetchall()

		logging.debug("Table desc: %s", table_desc)

		for col_desc in table_desc:
			col_name = col_desc[0]

			setattr(self, col_name, col_desc[4])
			self.db_attrs.append(col_name)

			self.attr_types[col_name] = col_desc[1]

			if col_desc[2] == 'YES':
				self.nullable_attrs.append(col_name)

			if col_desc[3] == 'PRI':
				self.pk_attrs.append(col_name)

			if col_desc[5] == 'auto_increment':
				self.seq = col_name

	def __setattr__(self, name, value):
		if name != 'db_attrs' and name in self.db_attrs:
			self.has_changed = True

		super(IfaceDB, self).__setattr__(name, value

	def insert(self):
		#A function to return 'null' if the inputted value is None. Otherwise return the inputted value.
		base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s);"
		complete_insert = base_insert % dict(
			table = self.table_name,
			cols  = ",".join([n for n in self.db_attrs]),
			vals  = ",".join([self.get_val(n) for n in self.db_attrs]),
		)

		logging.debug("Running insert: %s", complete_insert)
		old_seq = cursor.lastrowid
		
		cursor.execute(complete_insert)
		
		if old_seq is None or old_seq != cursor.lastrowid:
			setattr(self, self.seq, cursor.lastrowid)

	def update(self):
		#A function to return 'null' if the inputted value is None. Otherwise return the inputted value.

		base_update = "update %(table)s set %(sets)s where %(pk)s;"
		complete_update = base_update % dict(
			table = self.table_name,
			sets  = ",".join(["%s = %s"%(n, self.get_val(n)) for n in self.db_attrs]),
			pk    = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
		)
		logging.debug("Running update: %s", complete_update)
		cursor.execute(complete_update)

	def load(self):

		for pk in self.pk_attrs:
			if getattr(self, pk) is None:
				logging.info("Primary key is not set. Cannot load row from DB.")
				return None

		base_load = "select * from %(table_name)s where %(pk)s;"
		complete_load = base_load % dict(
			table_name = self.table_name,
			pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
		)
		logging.debug("Running load: %s", complete_load)
		cursor.execute(complete_load)
		result_row = cursor.fetchall()
		if len(result_row) == 0:
			raise DBException("No entry found for table")

	def delete(self):
		base_load = "delete from %(table_name)s where %(pk)s;"
		complete_delete = base_load % dict(
			table_name = self.table_name,
			pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
		)
		logging.debug("Running delete: %s", complete_delete)
		cursor.execute(complete_delete)
	
	def get_val(self, attr):
		val = getattr(self, attr)
		if val is None:
			return 'null'
		else:
			if self.attr_types[attr].find('varchar') >= 0:
				return "\'%s\'"%val
			else:
				return str(val)
	
class Project(IfaceBase):
	def __init__(self):
		IfaceBase.__init__(self, self.__class__.__name__)
