#dbaccessor.py

import json
import os
import sqlite3




class DbAccessorError(Exception):
  pass


class DbSchemaValidatorError(Exception): 
  pass


class DbSchemaValidator(object):
  '''
  This class instantiates a object that can be used to 
  determine the validity of table_names and field_names.
  The dbschema instance method is a dictionary. The keys 
  are valid table names, and the value of a key is an array
  of valid field_names for that table. Note that the dbschema 
  can go out-of-date if someone changes the database's schema 
  by creating or dropping tables, for example.
  '''
  def __init__(self, dbschema):
    self.dbschema = dbschema 

    table_field_dict = {}

    for table_name in dbschema.keys():
      field_name_list = [fname_ftype_list[0] for fname_ftype_list in dbschema[table_name]]
      table_field_dict[table_name] = field_name_list

    self.table_field_dict = table_field_dict



  def is_table(self, table_name): 
    return table_name in self.dbschema 


  def is_field(self, table_name, field_name): 
    if not self.is_table(table_name):
      raise DbSchemaValidatorError("table %s is not in dbschema." % table_name)  

    #field_name_list = [fnt[0] for fnt in self.dbschema[table_name]]
    field_name_list = self.table_field_dict[table_name]

    return field_name in field_name_list 

  def get_dbschema_json_str(self):
    return json.dumps(self.dbschema, indent=4,sort_keys=True)

  def display_dbschema(self):
    print(self.get_dbschema_json_str())






class DbAccessor(object):
  '''
  Class to create and execute SQL statements for
  data definition and data manipulation (CRUD). 
  '''

  def __init__(self, dbpath, **kwargs):

    self.dbpath = dbpath

    if not kwargs: kwargs = {}

    default_dict = {'new_db_ok': True, 'verbose': False}
    for k,v in default_dict.items():
      self.__dict__[k] = v if k not in kwargs else kwargs[k]


    try:

      if self.new_db_ok:
        self.conn = sqlite3.connect(self.dbpath)

      elif os.path.exists(self.dbpath):
        self.conn = sqlite3.connect(self.dbpath)

      else:
        raise IOError('Database not found: ' + self.dbpath)

    except Exception as detail:
        self.vprint("Exception: Unable to open db file: ", self.dbpath, " detail: ", detail)
        raise






  def vprint(self, *args):
    if self.verbose:
      pstr = ""
      for s in args: pstr += str(s) 
      print(s) 


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
      self.vprint("execute sqlite OperationalError: ", error)
      raise
    except sqlite3.DatabaseError as error:
      self.vprint("execute sqlite DatabaseError: ", error)
      raise
    except sqlite3.IntegrityError as error:
      self.vprint("execute sqlite IntegrityError: ", error)
      raise
    except sqlite3.ProgrammingError as error:
      self.vprint("execute sqlite ProgrammingError: ", error)
      raise
    except sqlite3.Error as error:
      self.vprint("execute sqlite Error: ", error)
      raise
    except Exception as error:
      self.vprint("execute Exception Error: ", error)
      raise

    return(result)

  def get_row_list_with_execute(self, stmt, params=[]):
    result = self.execute(stmt, params)
    return [row for row in result]

  def get_row_list_with_executemany(self, stmt, params=[]):
    result = self.executemany(stmt, params)
    return [row for row in result]



  #---------- Schema Examination Methods -------------------

  def get_table_names(self):
    rows = self.execute("select name from sqlite_master where type = 'table' ")
    return [row[0] for row in rows]


  def get_index_names(self):
    rows = self.execute("select name from sqlite_master where type = 'index' ")
    return [row[0] for row in rows]


  def get_field_names(self, table_name):
    cur = self.execute("select * from %s limit 0" % table_name)
    field_names = [desc[0] for desc in cur.description]
    cur.close()
    return(field_names)

  def get_field_name_type_list(self, table_name):
    '''
    returns a field name type list, with the form [(<field_name>,<field_type>), ...]
    Note that <field_type> just has the type information (e.g. 'integer').
    It does not have all the information on the field. So, for the 'id'
    field, it will NOT have 'integer primary key autoincrement not null'
    even though that may have been how the field was initially declared.
    Instead, if the id field is an integer, the tuple would be
    (u'id', u'integer')
    '''

    cur = self.execute("PRAGMA table_info(%s)" % table_name)
    field_name_type_list = [(row[1], row[2]) for row in cur]
    cur.close()
    return field_name_type_list

  def get_create_sql_dict(self):
    #returns dict in this form:
    # {<table_name>: <create_sql_stmt, ...}
    #create_sql_stmt looks like this
    #create_sql_stmt = 'CREATE TABLE stocks (id integer primary key autoincrement not null, \
    #ticker text unique, industry text, beta numeric, price numeric)'
    rows = self.execute("select name, sql from sqlite_master where type='table'")

    create_sql_dict = {}
    for row in rows:
      table_name = row[0]
      sql_create = row[1]
      create_sql_dict[table_name] = sql_create 


    return create_sql_dict

  def get_field_definition_list(self, sql):
    if not sql: return list()

    start = sql.find('(')
    end = sql.find(')')
    if start == -1 or end == -1: return list()

    match = sql[start+1:end]
    fdl = match.split(',')
    fdl = [e.strip() for e in fdl]

    return fdl



  def get_dbschema(self):
    dbschema = {}
    for tn in self.get_table_names(): 
      dbschema[tn] = self.get_field_name_type_list(tn)
    return(dbschema) 


  def get_db_validator(self):
    dbschema = self.get_dbschema() 
    return DbSchemaValidator(dbschema) 

  def get_dbschema_json_str(self):
    return json.dumps(self.get_dbschema(), indent=4,sort_keys=True)

  def display_dbschema(self):
    print(self.get_dbschema_json_str())

  #---------- Data Definition Methods ----------------------

  def create_table(self, table_name, field_names_types):  
    """ 
    makes a create command

    field_names_types should be ordered list of tuples [(<name>, <type>),...]
    note that you can put other information regarding the declaration
    of a field in the second element of the tuple.

    e.g. field_names_types = [
          ('id', 'integer primary key autoincrement not null'), 
          ('ticker', 'text'), ('beta', 'numeric'), 
          ('price', 'numeric')]

    if you use 'integer primary key autoincrement not null' 
    then an index will be created
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
  def get_dict_kv_lists(dict_table):
    klst, vlst = [],[]
    for k,v in dict_table.items():
      klst.append(k)
      vlst.append(v)
    return(klst, vlst)    

  @staticmethod
  def mkupdate(table, set_row, where_row_list):

    (set_key_list, value_list) = DbAccessor.get_dict_kv_lists(set_row)


    stmt = 'UPDATE ' + table + " \n"
    stmt += "SET "
    stmt += ', '.join([k + ' = ' + '?'  for k in set_key_list])

    (where_clause, where_value_list) = DbAccessor.mk_where_clause(where_row_list)
    stmt += where_clause
    stmt += ";"

    value_list.extend(where_value_list)

    return(stmt, value_list)


  def update(self, table, set_row, where_row_list):
    (stmt, value_list) = DbAccessor.mkupdate(table, set_row, where_row_list)
    self.execute( stmt, value_list )


  @staticmethod 
  def mk_where_clause(where_row_list):
    valid_comp_op = ('=', '>', '<', '>=', '<=', '!=', '<>')
    #where_row_list structure
    #[('industry', '=', 'technology'), ('beta', '>', 1.0), ...]

    if not where_row_list:
      where_clause = ""
      value_list = []

    else:
        for col in where_row_list:
          if col[1] not in valid_comp_op:
            raise DbAccessorError('bad compariso operator: %s' % col[1])

          if len(col) != 3:
            raise DbAccessorError('column tuple length not equal 3')

        where_clause = "\nWHERE "
        where_clause += "\n  AND ".join(["%s %s ?" % (col[0], col[1])  for col in where_row_list])

        value_list = [col[2] for col in where_row_list]

    return where_clause, value_list

  @staticmethod
  def mkdelete(table, where_row_list):
    stmt = "DELETE FROM " + table

    (where_clause, value_list) = DbAccessor.mk_where_clause(where_row_list)
    stmt += where_clause
    stmt += ";"
  
    return stmt, value_list 


  def delete(self, table, where_row_list=None):
    (stmt, value_list) = DbAccessor.mkdelete(table, where_row_list)
    self.execute(stmt,value_list)



  @staticmethod
  def mkselect(table, columns=None, where_row_list=None, sort_cols=None):

    vaild_sort_op = ('ASC', 'DESC')

    stmt = 'SELECT '
    if columns:
        stmt += ', '.join(columns)
    else:
        stmt += '*'

    # from clause
    stmt += "\nFROM " + table

    (where_clause, value_list) = DbAccessor.mk_where_clause(where_row_list)
    stmt += where_clause


    # order clause
    # sort_cols = [('name_last', 'ASC'), ('age', 'DESC')]
    if sort_cols:
      sort_list = []
      for col_name, sort_type in sort_cols: 
        if not sort_type.upper() in vaild_sort_op:
          raise DbAccessorError('bad sort type %s' % sort_type)
        else:
          sort_list.append(col_name + ' ' + sort_type)

      stmt += "\nORDER BY "
      stmt += ', '.join(sort_list)

    stmt += ';'

    return stmt, value_list




  def read(self, table, columns=None, where_row_list=None, sort_cols=None):
    '''
    Executes a SELECT statement against table.

    Arguments:
    table                 -- name of the table to be read

    columns (optional)    -- list of columns to be read from table

    where_row_list (optional)  -- list used to build WHERE clause
                            where_row_list=[('industry', '=', 'technology'), ('beta', '<', 1.0)]

                            These two arguments will be created from where_row:
                            stmt='select * from stocks where industry = ?, beta > ?'
                            value_list = ['technology', 1.0]

                            They will be executed with:
                            cur.execute(stmt, value_list)

    sort_cols (optional)  -- list of (column, order) pairs
                          used to specify order of the
                          rows returned. Needs to be of
                          the form ('<column>', 'ASC'|'DESC')

    Returns: rows returned from the SELECT statement.
    '''


    #Possible error checks: 
    # check to see if sort column names are in 
    # the field names for the table in the database
    #
    # Solution: make it possible for dbaccessor's user 
    # to create a data base validator object.
    #
    # See class: DbSchemaValidator

    if not columns: columns = self.get_field_names(table)

    (stmt, value_list) = DbAccessor.mkselect(table, columns, where_row_list, sort_cols)

    tuple_row_list = self.execute(stmt, value_list)

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


