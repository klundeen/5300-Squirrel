# 5300-Squirrel 
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022

Usage (argument is database directory):
```
$ ./sql5300 ~/cpsc5300/data
```
Valgrind:
```
valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```

## <span style="color:orange">Otoño Sprint </span>
<b> Authors : Vindhya Nair Lolakumari Jayachandran, Carter Martin</b>
### Milestone 3 : <i>Schema Storage</i> 
Milestone 3 utilizes the framework created in Milestones 1 and 2 to implement basic SQL table operations. The four table commands in this deliverable are <b>create table</b>, <b>drop table</b>, <b>show tables</b>, and <b>show columns</b>. 

- #### <b>Create Table</b><br/>
	Description: Creates a table in the DB with the specified column information.
    <br>Syntax:
    ```
    <table_definition> ::= CREATE TABLE <table_name> ( <column_definitions> )
    <column_definitions> ::= <column_definition> | <column_definition>, <column_definitions>
    <column_definition> ::= <column_name> <datatype>
    ```
    Example Output:
    ```
    SQL> create table foo (id int, data text, x integer, y integer, z integer)
    CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)
    created foo
    ```

- #### <b>Drop Table</b><br/>
	Description: Drops a table from the DB.
    <br>Syntax:
    ```
    <drop_table_statement> ::= DROP TABLE <table_name>
    ```
    Example Output:
    ```
    SQL> drop table foo
    DROP TABLE foo
    dropped foo
    ```
- #### <b>Show Tables</b><br/>
	Description: Shows all tables in the DB (helpful for examining the results of a create or drop table statement).
    <br>Syntax:
    ```
    <show_table_statement> ::= SHOW TABLES
    ```
    Example Output:
    ```
    SQL> show tables
    SHOW TABLES
    table_name 
    +----------+
    "foo" 
    successfully returned 1 rows
    ```
- #### <b>Show Columns</b><br/>
	Description: Shows the columns of a specified table in the DB.
    <br>Syntax:
    ```
    <show_columns_statement> ::= SHOW COLUMNS FROM <table_name>
    ```
    Example Output:
    ```
    SQL> show columns from _tables
    SHOW COLUMNS FROM _tables
    table_name column_name data_type 
    +----------+----------+----------+
    "_tables" "table_name" "TEXT" 
    successfully returned 1 rows
    
    SQL> show columns from foo
    SHOW COLUMNS FROM foo
    table_name column_name data_type 
    +----------+----------+----------+
    "foo" "id" "INT" 
    "foo" "data" "TEXT" 
    "foo" "x" "INT" 
    "foo" "y" "INT" 
    "foo" "z" "INT" 
    successfully returned 5 rows
    ```
### Milestone 4 : <i>Indexing</i>
Milestone 4 creates the framework for future implementation of of SQL indexing. <b>create index</b>, <b>show index</b>, and <b>drop index</b>
- #### <b>Create Index</b><br/>
	Description: Creates an index on the specified columns 
    <br>Syntax:
    ```
	<create_index_statement> ::= CREATE INDEX <index_name> ON <table_name> (<column_name>)
    ```
  	Example Output:
    ```
	SQL> create index fx on goober (x,y)
    CREATE INDEX fx ON goober USING BTREE (x, y)
    created index fx
    ```
- #### <b>Show Index</b><br/>
	Description: Shows the indexes associated with the columns in the table
    <br>Syntax:
    ```
	<show_index_statement> ::= SHOW INDEX FROM <table_name>
    ```
  	Example Output:
    ```
	SQL> show index from goober
    SHOW INDEX FROM goober
    table_name index_name column_name seq_in_index index_type is_unique 
    +----------+----------+----------+----------+----------+----------+
    "goober" "fx" "x" 1 "BTREE" true 
    "goober" "fx" "y" 2 "BTREE" true 
    successfully returned 2 rows    
    ```
- #### <b>Drop Index</b><br/>
	Description: Drops the specified index from the table 
    <br>Syntax:
    ```
	<drop_index_statement> ::= DROP INDEX <index_name> FROM <table_name>
    ```
  	Example Output:
    ```
  	SQL> drop index fx from goober
    DROP INDEX fx FROM goober
    dropped index fx  
    ```



