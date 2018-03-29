#dbaccessor_readme.txt

#A simple sqlite3 database accessor class in python



#----------------------------
#Tests can be run with:

$ python dbaccessor_tests.py


#-------------------------------------
#To import the dbaccessor class use this

from dbaccessor import DbAccessor 



#--------------------

#Here are some specific examples of using DbAccessor
#To create a database called 'definer.db' with a table called 'stocks' use this:

    dbpath = 'definer.db'
    
    table_name = 'stocks'
    
    field_names_types = [('id', 'integer primary key autoincrement not null'), 
         ('ticker', 'text unique'), ('industry','text'),
         ('beta', 'numeric'), 
         ('price', 'numeric')]

    db = DbAccessor(dbpath)
    
    db.create_table(table_name, field_names_types)  
    
#--------------------
#To create and then drop an index on the 'industry' column of the table do this:

  column_name = 'industry'
  db.create_index(table_name, column_name)

  db.drop_index(table_name, column_name)

#----------------------
#The DbAccessor object can create a DbSchemaValidator object for testing
#the validity of table names and field names while your program is
#running. (Note: this assumes that the structure of the database
#-- i.e. the data base schema -- does not change while your program
#is running.)

#This is how one gets a DbSchemaValidator object
dbv = db.get_db_validator()

#This will return True if 'stocks' is a table in the database
dbv.is_table('stocks') 

#This will return True if 'stocks' is a table in the database
#and the stocks table has the field ticker.
dbv.is_field('stocks','ticker') 

#This will raise an error if the table name does not exist
dbv.is_field('my_misspelled_table_name', 'ticker')


#the dbv object is based on the dbschema when dbv is instantiated. 
#So, if the database schema is changed (e.g. by dropping a table)
#then the dbv object will be inconsistent with the database schema.


#So, the dbv object will not remain valid if the structure of the
#database is changed (e.g. if someone has created or dropped tables)
#while your program is running. If you are the only one using the
#database, then you should be in control of when tables are created 
#or dropped.


#The moral of the story is to generate a dbv object with get_db_validator,
#but only use it to validate table and field names just prior to
#executing CRUD (i.e. insert, read, update, delete) commands.
    
#Note: checking for valid table and field names is one way to prevent 
#injection attacks on your code (i.e. when bad guys try to subvert
#your program if you give them the chance to enter malicious table
#names or field names).

#-------------------------------
#To insert values into the stocks table use this:

    initial_values = [
            {'ticker': 'ibm', 'industry': 'technology', 'beta': 1.1, 'price': 56},
            {'ticker': 'dal', 'industry': 'transportation', 'beta': 1.3, 'price': 34},
            {'ticker': 'xom', 'industry': 'energy', 'beta': 1.1, 'price': 56},
            {'ticker': 'appl', 'industry': 'technology', 'beta': 1.3, 'price': 34}]
            
    db.insert(table_name, initial_values)  
   

    
#---------------------------
#To read those records back use this:

    results = db.read(table_name, 
        where_row={'industry':'technology'}, 
        sort_cols=[('industry', 'DESC'), ('ticker', 'ASC')])

    for row in results: print(row)

#---------------------------
#To update those records use this:

  set_row = {'industry': 'finance', 'beta':3.0}
  where_row = {'ticker': 'ibm'}
  db.update(table_name, set_row, where_row)
  
#---------------------------
#To delete  records use this:  


  where_row = {'ticker': 'ibm'}
  db.delete(table_name, where_row)
    

#---------------------------------
#To drop a table do this:

     db.drop_table(table_name)
     
     
#---------------------------------
#To close the database connection do this:

    db.close()