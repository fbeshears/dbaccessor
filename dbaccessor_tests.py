#dbaccessor_tests.py
from dbaccessor import DbAccessor, DbSchemaValidatorError

#--------------  test get_field_definition_list -------
def t_get_field_definition_list(db, table_name):
  print("\n----------- test get_field_definition_list ----------------\n")
  create_sql_dict = db.get_create_sql_dict()

  print("create_sql_dict:")
  print(create_sql_dict)
  print("-----")

  if table_name in create_sql_dict:
    sql = create_sql_dict[table_name]
    print("create sql: %s" % sql )
  else:
    sql = None
    print("no create sql found for %s: " % table_name)

  fdl = db.get_field_definition_list(sql)
  if len(fdl) < 1:
    print("empty field_definition_list")
  else:
    print("\nfield_definition_list:")
    print(fdl)  

  print("\n----------- test get_field_definition_list end ----------------\n")

#--------------  make sql command statements -------
def t_mkselect(cls, table, columns):
  print("\n----- t_mkselect ----------\n")
  sort_cols = [('ticker', 'ASC'), ('age', 'DESC')]
  where_row_list = [('ticker', '=', 'ibm'), ('industry', '=', 'technology')]
  (stmt, value_list) = cls.mkselect(table, columns, where_row_list, sort_cols)
  print("t_mkselect SQL:\n" + stmt )
  print("value_list:")
  print(value_list)


def t_mkupdate(cls, table):
  print("\n----- t_mkupdate ----------\n")
  set_row = {'industry': 'finance', 'beta':3.0}
  where_row_list = [('ticker', '=', 'ibm'), ('industry', '=', 'technology')]
  (stmt, value_list) = cls.mkupdate(table, set_row, where_row_list)
  print("t_mkupdate SQL:\n" + stmt)
  print("value_list: \n", value_list)

def t_mkdelete(cls, table):
  print("\n----- t_mkdelete ----------\n")
  where_row_list = [('ticker', '=', 'ibm'), ('industry', '=', 'technology')]
  (stmt, value_list) = cls.mkdelete(table, where_row_list)
  print("t_mkdelete SQL:\n" + stmt)
  print("value_list: \n", value_list)

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
  field_names_types = [('id', 'integer primary key autoincrement not null'), 
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
  print("\n----- display dbschema ---------")
  db.display_dbschema()
  print("\n--------------------------")   


#-----------  DbSchema Validation Tests ------------------ 

def test_db_validator(db): 

  def t_table_name(dbv, table_name): 
    print("\n-----------------------")
    print("is table %s in the dbschema" % table_name) 
    if dbv.is_table(table_name): 
      print("table %s is in dbschema" % table_name) 
    else: 
      print("table %s is not in dbschema" % table_name)

    print("\n-------------------------\n")

  def t_field_name(dbv, table_name, field_name): 
    print("\n-----------------------")
    print("does table %s have field %s " % (table_name, field_name) )

    if dbv.is_field(table_name, field_name): 
      print("table %s does have field %s" % (table_name, field_name)) 
    else: 
      print("table %s does not have field %s" % (table_name, field_name))

    print("\n-------------------------\n")

  def t_does_is_field_crash(dbv,  invalid_table_name, field_name):

    print("------------  try to catch is_field exception ---------------\n")
    try:
      dbv.is_field(invalid_table_name, field_name)
      print("is_field not does catch invalid table_name: %s" % invalid_table_name)
      print("\n-------------------------\n")
    except DbSchemaValidatorError as error:
      print("DbSchemaValidatorError: %s" % error) 
      print("is_field does catch invalid table_name: %s" % invalid_table_name)
      print("test recovering without raising error")
      print("\n-------------------------\n")

  #begin test_db_validator
  print("\n\n===========================")
  print("tests of DbSchemaValidator object\n\n")

  dbv = db.get_db_validator() 

  valid_table_name = 'stocks' 
  valid_field_name = 'ticker'

  invalid_table_name = 'my_misspelled_table_name' 
  invalid_field_name = 'my_misspelled_field_name'

  print("--------- dbv.display_dbschema()---------------")
  dbv.display_dbschema()

  t_table_name(dbv,valid_table_name) 
  t_table_name(dbv, invalid_table_name) 

  t_field_name(dbv, valid_table_name, valid_field_name) 
  t_field_name(dbv, valid_table_name, invalid_field_name) 

  t_does_is_field_crash(dbv, invalid_table_name, valid_field_name)







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
    where_row_list=[('industry', '=', 'technology')], 
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
  where_row_list = [('ticker', '=', 'ibm')]
  db.update(table, set_row, where_row_list)

  db.display_table(table,"table with ibm updates")


def t_delete(db, table):
  where_row_list = [('ticker', '=', 'ibm')]
  db.delete(table, where_row_list)

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

  #default key word arguments are:  new_db_ok=True  and verbose=False 

  # db = DbAccessor(dbpath)  # will assume that the object will not make print stmts
  # db = DbAccessor(dbpath, verbose=False) #same as the default

  #In testing mode you may want all print statements 
  #Note that when dbaccessor hits an exception, it always raises it.

  db = DbAccessor(dbpath, new_db_ok=True, verbose=False)

  #----------  Display dbschema -----------------
  print("-----------  dbschema ------------------\n")
  db.display_dbschema() 

  #---------- Data Definition Method tests  ----------------------

  test_data_definitions(db, table)

  
  #----------  field_definition_list -----------------
  t_get_field_definition_list(db, table)


  #----------  Make stmt tests -------------------

  test_mk_sql_stmts(db, table)




  #----------- DbValidator object tests ----------------------

  test_db_validator(db)


  # #-----------  Data Manipulation Tests --------------------

  test_data_manipulation(db, table)


  db.close()


if __name__ == '__main__':
  main()