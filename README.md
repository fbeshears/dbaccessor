#dbaccessor

#A simple sqlite3 accessor class in python

#If you're using python to create and update a sqlite3 database, the DbAccessor class may be of some help.

#-------------------------------------

#To import the dbaccessor class use this

from dbaccessor import DbAccessor 

#----------------------------

#To see the tests brinng up the file dbaccessor_tests.py in your editor

#To run tests, see the following section at the bottom of dbaccessor.py

if __name__ == '__main__':

#here you can uncomment the test(s) you would like to run
#then you can just execute dbaccessor as follows:

$ python dbaccessor.py


#---------------------

#To create a database called 'definer.db' with a table called 'stocks' use this:

    dbpath = 'definer.db'
    
    table_name = 'stocks'
    
    field_names_types = [('id', 'integer primary key unique'), 
         ('ticker', 'text unique'), ('industry','text'),
         ('beta', 'numeric'), 
         ('price', 'numeric')]

    db = DbAccessor(dbpath)
    
    db.create_table(table_name, field_names_types)  
    
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