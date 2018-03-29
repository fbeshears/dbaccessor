#dbaccessor_tests.py
from dbaccessor import DbAccessor

#--------------  make sql command statements -------
def t_mkselect(cls, table, columns):
  print("\n----- t_mkselect ----------\n")
  sort_cols = [('ticker', 'ASC'), ('age', 'DESC')]
  where_row = {'ticker': 'ibm', 'industry': 'technology'}
  stmt = cls.mkselect(table, columns, where_row, sort_cols)
  print("t_mkselect SQL:\n" + stmt )


def t_mkupdate(cls, table):
  print("\n----- t_mkupdate ----------\n")
  set_row = {'industry': 'finance', 'beta':3.0}
  where_row = {'ticker': 'ibm', 'industry': 'technology'}
  (stmt, value_list) = cls.mkupdate(table, set_row, where_row)
  print("t_mkupdate SQL:\n" + stmt)
  print("value_list: \n", value_list)

def t_mkdelete(cls, table):
  print("\n----- t_mkdelete ----------\n")
  where_row = {'ticker': 'ibm', 'industry': 'technology'}
  stmt = cls.mkdelete(table, where_row)
  print("t_mkdelete SQL:\n" + stmt)

def t_mkinsert(cls, table):
  print("\n----- t_mkinsert ----------\n")
  initial_values = [
    {'ticker': 'ibm', 'industry': 'technology', 'beta': 1.1, 'price': 56},
    {'ticker': 'dal', 'industry': 'transportation', 'beta': 1.3, 'price': 34},
    {'ticker': 'xom', 'industry': 'energy', 'beta': 1.1, 'price': 56},
    {'ticker': 'appl', 'industry': 'technology', 'beta': 1.3, 'price': 34}]

  stmt = cls.mkinsert(table, initial_values)

  print("t_mkinsert SQL:\n" + stmt)


#----------   Data Definition Tests -------------------------

def t_create_table(db, table_name):
  field_names_types = [('id', 'integer primary key unique'), 
         ('ticker', 'text unique'), ('industry','text'),
         ('beta', 'numeric'), 
         ('price', 'numeric')]

  print("\n--------------------")
  print('creating table {0}'.format(table_name))
  db.create_table(table_name, field_names_types)  


def t_create_drop_index(db, table_name):
  print("\n--------------------")
  print("test of create_index and drop_index")

  column_name = 'industry'

  db.create_index(table_name, column_name)
  print(db.get_index_names())

  db.drop_index(table_name, column_name)
  print(db.get_index_names())
  print("\n--------------------")


def print_schema(db):
  print("\n----- get_dbschema ---------")
  print(db.get_dbschema())
  print("\n--------------------------")   


#-----------  Data Manipulation Tests --------------------


def initial_insert(db, table):
  print("\n===========  initial_insert begin ==========")

  db.display_table(table, "initial_table")

  db.delete(table)

  initial_values = [
            {'ticker': 'ibm', 'industry': 'technology', 'beta': 1.1, 'price': 56},
            {'ticker': 'dal', 'industry': 'transportation', 'beta': 1.3, 'price': 34},
            {'ticker': 'xom', 'industry': 'energy', 'beta': 1.1, 'price': 56},
            {'ticker': 'appl', 'industry': 'technology', 'beta': 1.3, 'price': 34}]

  db.insert(table, initial_values)  

  db.display_table(table, "table after deleting all rows and inserting %d rows" % len(initial_values) )
  print("\n===========  initial_insert end ==========\n\n")

def t_read_insert(db, table):

  print("\n===========  t_read_insert begin ==========")

  print ("\n---------  deleting all rows with id > 2  --------------\n")
  db.conn.execute("delete from stocks where id > 2")

  db.display_table(table, "table after delete")


  values =  [{'ticker': 'xom', 'industry': 'energy', 'beta': 1.1, 'price': 56},
            {'ticker': 'appl', 'industry': 'technology', 'beta': 1.3, 'price': 34}]

  db.insert(table, values)

  db.display_table(table, "table after insert of two new rows")


  print ("\n---------  table where industry='technology' -------------\n")
  results = db.read(table, 
    where_row={'industry':'technology'}, 
    sort_cols=[('industry', 'DESC'), ('ticker', 'ASC')])

  for row in results: print(row)

  print ("\n---------  sorted table  -------------\n")

  results = db.read(table, sort_cols=[('industry', 'DESC'), ('ticker', 'ASC')])

  for row in results: print(row)


  print ("\n---------  table with error  -------------\n")

  try:
    results = db.read(table, sort_cols=[('industry', 'what')])

  except Exception as e:
    print('Exception error: %s' % e)
    print("raised error and caught it")
    print("continuing without crashing")



  print("\n===========  t_read_insert end ==========\n\n")




def t_update(db, table):
  set_row = {'industry': 'finance', 'beta':3.0}
  where_row = {'ticker': 'ibm'}
  db.update(table, set_row, where_row)

  db.display_table(table,"table with ibm updates")


def t_delete(db, table):
  where_row = {'ticker': 'ibm'}
  db.delete(table, where_row)

  db.display_table(table,"updated table after deleting ibm")


def t_no_where_rows(db, table):

  print ("\n---------  select table no where_rows ---------------\n")
  for row in db.read(table): print(row)    

def test_data_definitions(db, table):
  print_schema(db)
  print("dropping table %s" % table)
  db.drop_table(table)

  print_schema(db)
  t_create_table(db, table)
  print_schema(db)
  t_create_drop_index(db, table)
  print_schema(db)

def test_data_manipulation(db, table):
  initial_insert(db, table)
  t_read_insert(db, table)
  t_no_where_rows(db, table)
  t_update(db, table)
  t_delete(db, table)  
  print("\n\n") 

def test_mk_sql_stmts(db, table):
  print("\n\n==============  make stmt tests begin ===============\n\n")
  columns = db.get_field_names(table)

  t_mkselect(DbAccessor, table, columns)

  t_mkupdate(DbAccessor, table)

  t_mkdelete(DbAccessor, table)

  t_mkinsert(DbAccessor, table)
  print("\n\n==============  make stmt tests end ===============\n\n")


def main():

  dbpath = 'definer.db'
  table = 'stocks'

  db = DbAccessor(dbpath)

  #----------  Make stmt tests -------------------

  test_mk_sql_stmts(db, table)

  #---------- Data Definition Method tests  ----------------------

  test_data_definitions(db, table)


  #-----------  Data Manipulation Tests --------------------

  test_data_manipulation(db, table)


  db.close()


if __name__ == '__main__':
  main()