# 5300-Squirrel

## Milestone 1
Milestone 1 is a SQL interpreter that accepts simple **SELECT** and **CREATE** Table queries.

**Instructions** 

To run the SQL interpreter:

1) Enter ```make```.

2) ```./sql5300 env_path (e.g. ./sql5300 ~/cpsc5300/5300-Squirrel)```

3) To quit, enter ```quit```.

**Sample**

```(sql5300: running with database environment at /home/st/smithj/sql5300env/data)
SQL> create table foo (a text, b integer, c double)
CREATE TABLE foo (a TEXT, b INT, c DOUBLE)
SQL> select * from foo left join goober on foo.x=goober.x
SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x
SQL> select * from foo as f left join goober on f.x = goober.x
SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x
SQL> select * from foo as f left join goober as g on f.x = g.x
SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x
SQL> select a,b,g.c from foo as f, goo as g
SELECT a, b, g.c FROM goo AS g, foo AS f
SQL> select a,b,c from foo where foo.b > foo.c + 6
SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6
SQL> select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1
SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1
SQL> foo bar blaz
Invalid SQL: foo bar blaz
SQL> quit
```

## Milestone 2
Milestone 2 is a simple relations manager that is built on top of ```DbBlock```, ```DbFile```, and ```DbRelation``` abstract base classes. The concrete implementations are ```SlottedPage```, ```HeapFile```, and ```HeapTable```. 

**Instructions**

To run the program, 

1) Enter ```make```.

2) Enter the SQL Interpreter by entering ```./sql5300 env_path (e.g. ./sql5300 ~/cpsc5300/5300-Squirrel```).

3) Enter ```test```.

**Sample**
```[vmarklynn@cs1 5300-Squirrel]$ ./sql5300 ~/cpsc5300/5300-Squirrel
/home/st/vmarklynn/cpsc5300/5300-Squirrel
(sql5300: running with database environment at /home/st/vmarklynn/cpsc5300/5300-Squirrel)
SQL> test
create ok
drop ok
create_if_not_exists ok
try insert
insert ok
select ok 1
project ok
tests passed
SQL> quit
```
