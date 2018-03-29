#dbaccessor.py

import os
import sqlite3


class DbAccessorError(Exception):
  pass


class DbAccessor(object):
  '''
  Class to handle data access 
  '''

  def __init__(self, dbpath, new_db_ok=True):

    def init_db(self, dbpath):
      self.dbpath = dbpath
      self.conn = sqlite3.connect(dbpath)   
        
    try:

      if new_db_ok:
        init_db(self, dbpath)

      elif os.path.exists(dbpath):
        init_db(self, dbpath)

      else:
        raise IOError('Database not found: ' + dbpath)

    except Exception as detail:
        print("Exception: Unable to open db file: " + dbpath + " detail: " + detail)
        raise



  def close(self):
    self.conn.close()



  def display_table(self, table, title="table"):

    print ("\n---------  %s  ---------------\n" % title)
    for row in self.read(table): print(row)

  #---------- execute within context  ----------------------

  def execute(self, stmt, params=[]):
    return self.try_execute(self.conn.execute, stmt, params)

  def executemany(self, stmt, params=[]):
    return self.try_execute(self.conn.executemany, stmt, params)

  def try_execute(self, execute, stmt, params=[]):
    if params == None: params=[]

    try:
      
      with self.conn:
        result = execute(stmt, params)

    except sqlite3.OperationalError as error:
      print("execute sqlite OperationalError: ", error)
      raise
    except sqlite3.DatabaseError as error:
      print("execute sqlite DatabaseError: ", error)
      raise
    except sqlite3.IntegrityError as error:
      print("execute sqlite IntegrityError: ", error)
      raise
    except sqlite3.ProgrammingError as error:
      print("execute sqlite ProgrammingError: ", error)
      raise
    except sqlite3.Error as error:
      print("execute sqlite Error: ", error)
      raise
    except Exception as error:
      print("execute Exception Error: ", error)
      raise

    return(result)



  #---------- Schema Examination Methods -------------------

  def get_table_names(self):
    rows = self.execute("select name from sqlite_master where type = 'table' ")
    return [row[0] for row in rows]


  def get_index_names(self):
    rows = self.execute("select name from sqlite_master where type = 'index' ")
    return [row[0] for row in rows]


  def get_field_names(self, table_name):
    cur = self.execute("select * from %s limit 1" % table_name)
    field_names = [desc[0] for desc in cur.description]
    cur.close()
    return(field_names)


  def get_dbschema(self):
    dbschema = {}
    for tn in self.get_table_names(): 
      dbschema[tn] = self.get_field_names(tn)
    return(dbschema)


  #---------- Data Definition Methods ----------------------

  def create_table(self, table_name, field_names_types):  
    """ makes a create command
    field_names_types should be ordered list of tuples
    e.g. field_names_types = [('id', 'integer primary key'), 
         ('ticker', 'text'), ('beta', 'numeric'), 
         ('price', 'numeric')]
    if you use 'integer primary key unique' then an idex will be created
    """

    fnt_list = ["%s %s" % (fn, ft) for fn, ft in field_names_types]
    fntypes = ", ".join(fnt_list)

    stmt = "create table if not exists %s (%s)" % (table_name, fntypes)
    self.execute(stmt)


  def drop_table(self, table_name):
    stmt = "drop table if exists %s" % table_name
    self.execute(stmt)


  @staticmethod
  def create_index_name(table_name, index_field_name):
    return "%s_%s_index" % (table_name, index_field_name)


  def create_index(self, table_name, index_field_name):
    index_name = DbAccessor.create_index_name(table_name, index_field_name)
    stmt = "create unique index %s on %s(%s)" % (index_name, table_name, index_field_name)

    self.execute(stmt)



  def drop_index(self, table_name, index_field_name):
    index_name = DbAccessor.create_index_name(table_name, index_field_name)
    stmt = "drop index if exists %s" % index_name

    self.execute(stmt)



  #---------  Data Access Methods (CRUD) -------------------



  @staticmethod
  def mkupdate(table, set_row, where_row):

    def get_lists(dict_table):
      klst, vlst = [],[]
      for k,v in dict_table.items():
        klst.append(k)
        vlst.append(v)
      return(klst, vlst)

    (set_key_list, set_value_list) = get_lists(set_row)
    (where_key_list, where_value_list) = get_lists(where_row)

    value_list = set_value_list
    value_list.extend(where_value_list)


    stmt = 'UPDATE ' + table + " \n"
    stmt += "SET "
    stmt += ', '.join([k + ' = ' + '?'  for k in set_key_list])
    stmt += "\nWHERE "
    stmt += "\n AND ".join([k + ' = ' + '?'  for k in where_key_list])
    stmt += ";"

    return(stmt, value_list)


  def update(self, table, set_row, where_row):
    (stmt, value_list) = DbAccessor.mkupdate(table, set_row, where_row)
    self.execute( stmt, value_list )



  @staticmethod
  def mkdelete(table, where_row):
    stmt = "DELETE FROM " + table

    if where_row:
      stmt += "\nWHERE "
      stmt += "\n  AND ".join([col + "=:" + col for col in where_row.keys()])
      stmt += ';'
    # else:
    #   where_row = {}
  
    return(stmt)

  def delete(self, table, where_row=None):
    stmt = DbAccessor.mkdelete(table, where_row)
    self.execute(stmt,where_row)



  @staticmethod
  def mkselect(table, columns=None, where_row=None, sort_cols=None):


      stmt = 'SELECT '
      if columns:
          stmt += ', '.join(columns)
      else:
          stmt += '*'

      # from clause
      stmt += "\nFROM " + table

      #------------------------------
      #where clause formatting
      #where_rows =  {"who": 'beshears', "age": 65}
      #
      #This is the named style (note db columns don't have to match dict keys)
      #
      #cur.execute("select * from people where name_last=:who and age=:age", where_rows)
      #
      #-----------------------------


      if where_row:
          stmt += "\nWHERE "
          stmt += "\n  AND ".join([col + "=:" + col for col in where_row.keys()])

      # order clause
      # sort_cols = [('name_last', 'ASC'), ('age', 'DESC')]
      if sort_cols:
        sort_list = []
        for col_name, sort_type in sort_cols: 
          if not sort_type.upper() in ['ASC', 'DESC']:
            raise DbAccessorError('bad sort type')
          else:
            sort_list.append(col_name + ' ' + sort_type)

        stmt += "\nORDER BY "
        stmt += ', '.join(sort_list)

      stmt += ';'


      return stmt


  def read(self, table, columns=None, where_row=None, sort_cols=None):
    '''
    Executes a SELECT statement against table.

    Arguments:
    table                 -- name of the table to be read

    columns (optional)    -- list of columns to be read from table

    where_row (optional)  -- dict used to build WHERE clause
                            stmt='select * from stocks where industry=:industry'
                            where_row={'industry':'technology'}
                            cur.execute(stmt, where_row)

    sort_cols (optional)  -- list of (column, order) pairs
                          used to specify order of the
                          rows returned. Needs to be of
                          the form ('<column>', 'ASC'|'DESC')

    Returns: rows returned from the SELECT statement.
    '''


    #Possible error checks: 
    # 1. check to see if sort column names are in 
    #    the field names for the table in the database

    if not columns: columns = self.get_field_names(table)

    stmt = DbAccessor.mkselect(table, columns, where_row, sort_cols)

    tuple_row_list = self.execute(stmt, where_row)

    result_list = [dict(zip(columns, trow)) for trow in tuple_row_list]


    return(result_list)



  @staticmethod
  def mkinsert(table, values):
    # build list of column names
    cols = values[0].keys()

    stmt = 'INSERT INTO ' + table + ' ('
    stmt += ', '.join(cols)
    stmt += ') VALUES ('
    stmt += ', '.join([":%s" % col for col in cols])
    stmt += ')'
    stmt += ';'

    return(stmt)    


  def insert(self, table, values):
    '''
    Executes an INSERT statement against table.

    Arguments:
    table           -- name of the table to be written to
    values          -- list of rows (dicts) to be inserted

    Returns: None
    '''
    stmt = DbAccessor.mkinsert(table, values)
    self.executemany(stmt, values)


