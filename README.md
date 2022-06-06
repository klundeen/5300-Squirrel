# 5300-Squirrel
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4_prep</code> has the instructor-provided files for Milestone 4. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4</code> has the instructor's attempt to complete both the Milestone 3 and Milestone 4 assignments.
- <code>Milestone5_prep</code> has the instructor-provided files for Milestone5.
## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```
## Sprint Invierno
#### Authors: Binh Nguyen, Terence Leung
### Milestone 5: Insert, Delete, Simple Queries
#### Insert
For insert, the job is to construct the ValueDict row to insert and insert it. Also, make sure to add to any indices. Useful methods here are get_table, get_index_names, get_index. Make sure to account for the fact that the user has the ability to list column names in a different order than the table definition. 

Example output:
<pre>
SQL> insert into goober (z,y,x) VALUES (9,8,7)
INSERT INTO goober (z, y, x) VALUES (9, 8, 7)
successfully inserted 1 row into goober and 2 indices
SQL> select * from goober
SELECT * FROM goober
x y z 
+----------+----------+----------+
4 5 6 
9 9 9 
7 8 9 
successfully returned 3 rows 
</pre>

#### Select
For select, the job is to create an evaluation plan and execute it. Start the plan with a TableScan

Note that we then wrap that in a Select plan. The get_where_conjunction is just a local recursive function that pulls apart the parse tree to pull out any equality predicates and AND operations (other conditions throw an exception). Next we would wrap the whole thing in either a ProjectAll or a Project. Note that the new method has added to DbRelation, get_column_attributes(ColumnNames) that will get the attributes (for returning in the QueryResults) based on the projected column names.

Next we optimize the plan and evaluate it to get our results for the user.

Example output:
<pre>
SQL> select * from foo
SELECT * FROM foo
id data 
+----------+----------+
1 "one" 
2 "Two" 
3 "Three" 
99 "wowzers, Penny!!" 
successfully returned 4 rows
SQL> select * from foo where id=3
SELECT * FROM foo WHERE id = 3
id data 
+----------+----------+
3 "Three" 
successfully returned 1 rows
</pre>

#### Delete
Delete is a lot like select except that we don't wrap the evaluation plan into a final Project, since we want the pipeline results (which is a handle iterator instead of the values from the rows). 

For delete, we also have to remember to delete from any indices that are present. For this, make sure to delete from the indices before deleting from the table, since the index has the right to fetch the record from the table if we needs it for any reason during the delete.

Example output:
<pre>
SQL> delete from foo where id=1
DELETE FROM foo WHERE id = 1
successfully deleted 1 rows from foo and 2 indices
</pre>

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.
