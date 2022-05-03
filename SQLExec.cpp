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

QueryResult::~QueryResult() {
    // Destructor class
    if (column_names!=nullptr)
        delete column_names;
    if (column_attributes!=nullptr)
        delete column_attributes;
    if (rows!=nullptr){
        for(auto row: *rows)
            delete row;
        delete rows;

    }

}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables=new Tables();
   
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME ............. Vindhya will fix this
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    return new QueryResult("not implemented"); // FIXME............. Carter will fix this
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    return new QueryResult("not implemented"); // FIXME.............Carter will fix this
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    // Fixed by including the case for show Table and Show Column
    switch(statement->type){
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        

    }
    return new QueryResult("not implemented");
}

QueryResult *SQLExec::show_tables() {
    // Initializing variables for system two tables  _tables and _columns.......... Vindhya fixed this
    ColumnNames* column_names = new ColumnNames();
    column_names->push_back("table_name");
    ColumnAttributes* column_attributes = new ColumnAttributes();
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    


    Handles* handles = SQLExec::tables->select();
    int size=handles->size();
    ValueDicts *value_dict= new ValueDicts();
    


    
    for (auto const &handle : *handles){
        ValueDict *row = SQLExec::tables->project(handle,column_names);
        Identifier table_name = row->at("table_name").s;
        // Check if already tablename is present in schema table
        if(table_name!=Tables::TABLE_NAME && table_name !=Columns::TABLE_NAME){
        value_dict->push_back(row);
        }    

    }
    cout<<handles;
    delete handles;
    return new QueryResult(column_names,column_attributes,value_dict,"successfully returned "+to_string(size)+" rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ColumnNames* column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("datatype");
    // FIXME-------------- Need to implement remaining logic.......... Vindhya will fix this

    return new QueryResult("not implemented"); 
}

