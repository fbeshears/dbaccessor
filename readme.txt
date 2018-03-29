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
    
    field_names_types = [('id', 'integer primary key unique'), 
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