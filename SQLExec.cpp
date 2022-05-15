/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 * @author Vindhya Nair Lolakumari Jayachandran
 * @author Martin Carter S
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult()
{
    // Destructor class
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr)
    {
        for (auto row : *rows)
            delete row;
        delete rows;
}
}

QueryResult *SQLExec::execute(const SQLStatement *statement)
{
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    
    // initialize _indices table which will have details about index, if not yet present
    if (SQLExec::indices== nullptr)
        SQLExec::indices = new Indices();

    try
    {
        switch (statement->type())
        {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    }
    catch (DbRelationError &e)
    {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
    column_name = col->name;
    switch (col->type)
    {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    default:
        throw SQLExecError("invalid Column type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement)
{
    switch (statement->type)
    {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default :
        return new QueryResult("Create type not recognised");
    }
}

// Case for Create Table Statement
QueryResult *SQLExec::create_table(const CreateStatement *statement)
{
    if (statement->type != CreateStatement::kTable)
        return new QueryResult("Create type not table");
    Identifier table_name = statement->tableName;
    // update schema
    ValueDict row;
    row["table_name"] = table_name;
    Handle table_handle = SQLExec::tables->insert(&row);
    // initialize variables for column info
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    // update for each column in the input statement
    for (ColumnDefinition *curr_column : *statement->columns)
    {
        column_definition(curr_column, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
}

    try
    {
        Handles column_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try
        {
            // insert into the column handle
            for (uint i = 0; i < column_names.size(); ++i)
            {
                row["column_name"] = column_names[i];
                if (column_attributes[i].get_data_type() == ColumnAttribute::INT)
                    row["data_type"] = Value("INT");
                else
                    row["data_type"] = Value("TEXT");
                column_handles.push_back(columns.insert(&row));
            }
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        }
        catch (...)
        {
            try
            {
                for (uint i = 0; i < column_names.size(); ++i)
                    columns.del(column_handles.at(i));
            }
            catch (...)
            {
            }
            throw;
        }
    }
    catch (...)
    {
        try
        {
            SQLExec::tables->del(table_handle);
        }
        catch (...)
        {
        }
        throw;
    }
    return new QueryResult("created " + table_name);
}

// We'll use the following columns in _indices to create index for specified table:

// table_name - Name of the table which is indexed with this index
// index_name - Name of the index
// seq_in_index - Order of columns in a composite index
// column_name - Name of the column
// index_type - Either "BTREE" or "HASH"
// is_unique - True if the rows in the underlying relation are unique on this index's key, false otherwise. For now, we will assume true if USING BTREE and false if USING HASH.

QueryResult *SQLExec::create_index(const CreateStatement *statement)
{
    Identifier indexName = statement->indexName;
    Identifier tableName = statement->tableName;

    DbRelation& table = SQLExec::tables->get_table(tableName);
    const ColumnNames& table_columns = table.get_column_names();
    for(auto const& col_name: *statement->indexColumns){
        if(find(table_columns.begin(),table_columns.end(),col_name)==table_columns.end()){
            throw SQLExecError(string("column '"+string(col_name)+"' does not exist"));
        }
    }


    // insert a row for every column mentioned in above comments, they are basically columns in index which we insert into  _indices
    ValueDict row;
    row["table_name"] = Value(tableName);
    row["index_name"] = Value(indexName);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE");
    int sq_in_index = 0;
    Handles handles;
    try
    {
        // To get the column_name
        for (auto const &col_name : *statement->indexColumns)
        {
            row["seq_in_index"] = Value(++sq_in_index);
            row["column_name"] = Value(col_name);
            handles.push_back(SQLExec::indices->insert(&row));
        }
        // Creating Index
        DbIndex &index = SQLExec::indices->get_index(tableName, indexName);
        index.create();
    }
    catch (...)
    {
        try
        {
            for (auto const &handle : handles)
                SQLExec::indices->del(handle);
        }
        catch (...)
        {
        }
        throw;
    }
    return new QueryResult("created index "+ indexName);
}

QueryResult *SQLExec::drop(const DropStatement *statement)
{
    if (statement->type != hsql::DropStatement::kTable)
        return new QueryResult("Drop type not table");
    Identifier table_name = statement->name;
    ValueDict where;
    where["table_name"] = Value(table_name);
    // Checking if the table is a schema table
    if (Value(statement->name) == Tables::TABLE_NAME || Value(statement->name) == Columns::TABLE_NAME)
        return new QueryResult("Can not drop a schema table");
    DbRelation &table = SQLExec::tables->get_table(table_name);
    DbRelation &column = SQLExec::tables->get_table(Columns::TABLE_NAME);
    // removing from _column
    Handles *column_handles = column.select(&where);
    for (uint i = 0; i < column_handles->size(); ++i)
        column.del(column_handles->at(i));
    delete column_handles;
    // removing from _tables
    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());
    return new QueryResult("dropped  " + table_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement)
{
    // Fixed by including the case for show Table and Show Column
    switch (statement->type)
    {
    case ShowStatement::kTables:
        return show_tables();
    case ShowStatement::kColumns:
        return show_columns(statement);
    case ShowStatement::kIndex:
            return show_index(statement);
    }
    return new QueryResult("not implemented");
}
QueryResult *SQLExec::show_tables()
{
    // Initializing variables for two system tables  _tables and _columns
    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    Handles *handles = SQLExec::tables->select();
    int size = handles->size() - 2;
    ValueDicts *value_dict = new ValueDicts();
    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        // Check if already tablename is present in schema table
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
        {
            value_dict->push_back(row);
        }

    }
    // cout<<handles<<"\n";
    delete handles;
    return new QueryResult(column_names, column_attributes, value_dict, "successfully returned " + to_string(size) + " rows");
}
// This is the function to store column name and column data types and also returns the number of rows in system table _column
QueryResult *SQLExec::show_columns(const ShowStatement *statement)
{
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    int size = handles->size();
    ValueDicts *rows = new ValueDicts;
    for (auto const &handle : *handles)
    {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(size) + " rows");
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    
    // Same process as we did for show tables, only difference is here we are do for index too.
    ColumnNames *column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes *column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"]=Value(statement->tableName);
    Handles* handles = SQLExec::indices->select(&where);
    u_long size = handles->size();
    ValueDicts *value_dict = new ValueDicts;
    for (auto const &handle : *handles)
    {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        value_dict->push_back(row);
        }
    // cout<<handles<<"\n";
    delete handles;
    return new QueryResult(column_names, column_attributes, value_dict, "successfully returned " + to_string(size) + " rows");
 }

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // Carter will fix
}

//All these test cases are passing, which was required for MileStone3
// SQL> show tables
// SHOW TABLES
// table_name 
// +----------+
// successfully returned 0 rows
// SQL> show columns from _tables
// SHOW COLUMNS FROM _tables
// table_name column_name data_type 
// +----------+----------+----------+
// "_tables" "table_name" "TEXT" 
// successfully returned 1 rows
// SQL> show columns from _columns
// SHOW COLUMNS FROM _columns
// table_name column_name data_type 
// +----------+----------+----------+
// "_columns" "table_name" "TEXT" 
// "_columns" "column_name" "TEXT" 
// "_columns" "data_type" "TEXT" 
// successfully returned 3 rows
// SQL> create table foo (id int, data text, x integer, y integer, z integer)
// CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)
// created foo
// SQL> create table foo (goober int)
// CREATE TABLE foo (goober INT)
// Error: DbRelationError: foo already exists
// SQL> create table goo (x int, x text)
// CREATE TABLE goo (x INT, x TEXT)
// Error: DbRelationError: duplicate column goo.x
// SQL> show tables
// SHOW TABLES
// table_name 
// +----------+
// "foo" 
// successfully returned 1 rows
// SQL> show columns from foo
// SHOW COLUMNS FROM foo
// table_name column_name data_type 
// +----------+----------+----------+
// "foo" "id" "INT" 
// "foo" "data" "TEXT" 
// "foo" "x" "INT" 
// "foo" "y" "INT" 
// "foo" "z" "INT" 
// successfully returned 5 rows
// SQL> drop table foo
// DROP TABLE foo
// dropped  foo
// SQL> show tables
// SHOW TABLES
// table_name 
// +----------+
// successfully returned 0 rows
// SQL> show columns from foo
// SHOW COLUMNS FROM foo
// table_name column_name data_type 
// +----------+----------+----------+
// successfully returned 0 rows
// 
