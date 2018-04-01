# DbAccessor

A simple sqlite3 database accessor class written in python.


```python
from dbaccessor import DbAccessor

dbpath = 'definer.db'
db = DbAccessor(dbpath)

# create a stocks table

table_name = 'stocks'

field_names_types = 
  [('id', 'integer primary key autoincrement not null'), 
  ('ticker', 'text unique'), ('industry','text'),
  ('beta', 'numeric'), 
  ('price', 'numeric')]


db.create_table(table_name, field_names_types) 

```

Main DbAccessor data definition methods:
* create_table(table_name, field_name_types)
* drop_table(table_name)
* create_index(table_name, index_field_name)
* drop_index(table_name, index_field_name)

Main DbAccessor data manipulation methods (CRUD):
* insert(table_name, values)
* read(table_name, columns, where_row_list, sort_cols)
* update(table_name, set_row, where_row_list)
* delete(table_name, where_row_list)

```python
#Create and then drop an index on the 'industry' column:

column_name = 'industry'
db.create_index(table_name, column_name)

db.drop_index(table_name, column_name)
```

```python
#Insert values

initial_values = [
    {'ticker': 'ibm', 'industry': 'technology', 'beta': 1.1, 'price': 56},
    {'ticker': 'dal', 'industry': 'transportation', 'beta': 1.3, 'price': 34},
    {'ticker': 'xom', 'industry': 'energy', 'beta': 1.1, 'price': 56},
    {'ticker': 'appl', 'industry': 'technology', 'beta': 1.3, 'price': 34}]
        
db.insert(table_name, initial_values)  
```

    
```python
#Read print records

columns = None
where_row_list = [('industry', '=', 'technology')]
sort_cols = [('industry', 'DESC'), ('ticker', 'ASC')]

results = db.read(table_name, columns, where_row_list, sort_cols)

#Print records (each record is a dict)
for row in results: 
  print(row)
```

```
#Update a record

set_row = {'industry': 'finance', 'beta':3.0}
where_row_list = [('ticker', '=', 'ibm')]

db.update(table_name, set_row, where_row_list)

```

```
#Delete a record


where_row_list = [('ticker', '=', 'ibm')]
db.delete(table_name, where_row_list)
```


```
#Drop a table 

db.drop_table(table_name)
     
```

```
#Close the database connection 

db.close()
```

# dbSchemaValidator

The DbAccessor object can create a DbSchemaValidator object for testing
the validity of table names and field names while your program is
running. 

Note: this assumes that the structure of the database -- i.e. the data base schema -- does not change while your program is running. If the structure of the database does change, then the DbSchemaValidator object will be out-of-date.

```
#Gets a DbSchemaValidator object
dbv = db.get_db_validator()

#Return True if 'stocks' is a table in the database
dbv.is_table('stocks') 

#Return True if 'stocks' is a table in the database
#and the stocks table has the field named 'ticker'.
dbv.is_field('stocks','ticker') 

#Will raise an error if the table name does not exist
dbv.is_field('my_misspelled_table_name', 'ticker')
```

## Notes

1. The dbv object is based on the dbschema when dbv is instantiated. 

2. So, if the database schema is changed (e.g. by dropping a table) then the dbv object will be inconsistent with the database schema.

3. So, the dbv object will not remain valid if the structure of the database is changed (e.g. if someone has created or dropped tables) while your program is running. 

4. If you are the only one using the database, then you should be in control of when tables are created or dropped.

5. The moral of the story: generate a dbv object with get_db_validator, but only use it to validate table and field names just prior to executing CRUD commands (i.e. insert, read, update, delete).
    
6. Note: checking for valid table and field names is one way to prevent injection attacks on your code (i.e. when bad guys try to subvert your program if you give them the chance to enter malicious table names or field names).


