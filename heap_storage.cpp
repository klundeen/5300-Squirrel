#include "heap_storage.h"
#include <cstring>

typedef u_int16_t u16;

bool test_heap_storage() { return true; }

/*
  Slotted Page
  TODO:
    virtual void put(RecordID record_id, const Dbt &data);
*/
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false)
{
  if (is_new)
  {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  }
  else
  {
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt *data)
{
  if (!has_room(data->get_size()))
    throw DbBlockNoRoomError("Not enough room for new record");
  u16 id = ++this->num_records;
  u16 size = (u16)data->get_size();
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcopy(this->address(loc), data->get_data(), size);
  return id;
}

/*
  Handy reference: https://web.stanford.edu/class/cs276a/projects/docs/berkeleydb/api_cxx/dbt_class.html
*/
Dbt *SlottedPage::get(RecordID record_id)
{
  u16 size, loc;
  get_header(size, loc, record_id);
  if (size == 0)
    return;
  return new Dbt(this->address(loc), size)
}

void SlottedPage::put(RecordID record_id, const Dbt &data)
{
  u16 size, loc;
  get_header(size, loc, id);
  u16 newSize = data.get_size();
  // Can't replace; need to add new entry
  if (newSize > size)
  {
    u16 extra = newSize - size;
    if (!this->has_room(extra))
      new DbBlockNoRoomError('Not Enough room in block!');
    this->slide(loc + newSize, loc + size);
    memcpy(this->address(loc - extra), data.get_data(), newSize);
  }
  else 
  {
    memcpy(this->address(loc), data.get_data(), newSize);
    slide(loc + newSize, loc + size);
  }
  get_header(size, loc, record_id);
  put_header(record_id, new_size, loc);


}

void SlottedPage::del(RecordID record_id)
{
  u16 size, loc;
  get_header(size, loc, record_id);
  // Tombstoned here
  put_header(record_id, 0, 0);
  slide(loc, loc + size)
}

RecordIDs *SlottedPage::ids(void)
{
  RecordIDs *ids = new RecordIDs;
  for (u16 id = 1; id <= this->num_records; id++)
  {
    // ensures that record is not tombstoned
    if (get_n(4 * id) != 0)
      ids->push_back(id);
  }
  return ids;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0)
{
  size = get_n(4 * id);
  loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
  if (id == 0)
  {
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(4 * id, size);
  put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size)
{
  u16 availableSpace = this->end_free - (this->num_records + 1) * 4;
  return availableSpace >= size;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
  u16 size, loc;
  u16 shift = end - start;
  u16 itemsToMove = start - (end + 1);
  if (shift == 0)
    return;
  memcopy(this->address(shift + end_free + 1), this->address(end_free + 1), itemsToMove);

  RecordIDs *currentRecord = this->ids();
  for (RecordID &id : *currentRecord)
  {
    get_header(size, loc, id);
    if (loc <= start)
    {
      loc += shift;
      this->put_header(id, size, loc)
    }
  }
  delete currentRecord;
  this->end_free += shift;
  this->put_header()
}

// Get 2-byte integer at given offset in block
u_int16_t SlottedPage::get_n(u16 offset)
{
  return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block
void SlottedPage::put_n(u16 offset, u16 n)
{
  *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block
void *SlottedPage::address(u16 offset)
{
  return (void *)((char *)this->block.get_data() + offset);
}

/*
 * HeapFile
 * Constructor and deconstructor already defined in header file
 */
void HeapFile::create()
{
  this->db_open(DB_CREATE | DB_EXCL);
  SlottedPage *page = this->get_new();
  this->put(page);
}

void HeapFile::drop()
{
  this->close();
  if (std::remove(this->dbfilename.c_str()) != 0)
    throw std::runtime_error("Failed to remove physical file of the heapFile: " + dbfilename);
}

void HeapFile::open()
{
  this->db_open();
}

void HeapFile::close()
{
  if (not this->closed)
  {
    this->db.close(0);
    this->closed = true;
  }
}

void HeapFile::put(DbBlock *block)
{
  BlockID id = block->get_block_id();
  Dbt pair(&id, sizeof(id));
  this->db.put(nullptr, &pair, block->get_block(), 0);
}

void HeapFile::db_open(uint flags)
{
  // make sure heap file is not closed
  if (not this->closed)
    return;

  this->db.set_re_len(DbBlock::BLOCK_SZ);
  // db.open return 0 when success, so if not success we close the database.
  if (this->db.open(nullptr, dbfilename.c_str(), name.c_str(), DB_RECNO, flags, 0) != 0)
  {
    this->close();
    return;
  }
  this->closed = false;
}

BlockIDs *HeapFile::block_ids()
{
  BlockIDs *block_ids = new BlockIDs;
  for (BlockID i = 1; i < this->last + 1; i++)
    block_ids->push_back(i);
  return block_ids;
}

SlottedPage *HeapFile::get_new()
{
  this->last++;
  // creates a new block and insert to database
  u_int16_t size = DbBlock::BLOCK_SZ;
  char initial[size];
  Dbt pair(&this->last, size);
  Dbt content(initial, size);
  this->db.put(nullptr, &pair, &content, 0);
  // return a slottpage based on the new inserted block
  return new SlottedPage(content, this->last, true);
}

SlottedPage *HeapFile::get(BlockID block_id)
{
  Dbt pair(&block_id, sizeof(block_id));
  Dbt content;
  this->db.get(nullptr, &pair, &content, 0);
  return new SlottedPage(content, block_id, false);
}

/*
 * Heaptable class
 */

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
	DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

void HeapTable::create()
{
    file.create();
}

void HeapTable::create_if_not_exists()
{
    try
    {
	this->file.open();
    } 
    catch (...)
    {
	this->file.create();
    }
}

void HeapTable::drop()
{
    this->file.drop();
}

void HeapTable::open()
{
    this->file.open();
}

void HeapTable::close()
{
    this->file.close();
}

ValueDict* HeapTable::validate(const ValueDict* row)
{
    ValueDict* all_records = new ValueDict();
    for(auto const &column_name : this->column_names)
    {
	ValueDict::const_iterator column = row->find(column_name);
	if(column == row->end()) throw DbRelationError("Don't know how to handle NULLS, defaults, etc, yet");
	(*all_records)[column_name] = column->second;
    }
    return all_records;
}

Handle HeapTable::insert(const ValueDict* row)
{
    this->open();
    return this->append(this->validate(row));
}

//provided in milestone2 
Dbt* HeapTable::marshal(const ValueDict* row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    for(auto const& column_name: this->column_names)
    {
	ColumnAttribute ca = this->column_attributes[col_num++];
	ValueDict::const_iterator column = row->find(column_name);
	Value value = column->second;
	if(ca.get_data_type() == ColumnAttribute::DataType::INT)
	{
	    *(int32_t*) (bytes + offset) = value.n;
	}
	else if(ca.get_data_type() == ColumnAttribute::DataType::TEXT)
	{
    	    uint size = value.s.length();
	    *(u16*) (bytes + offset) = size;
	    offset += sizeof(u16);
	    memcpy(bytes + offset, value.s.c_str(), size);
	    offset += size;
	}
	else
	{
	    throw DbRelationError("Only know how to marshal INT and TEXT");
	}
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data)
{
    ValueDict *row = new ValueDict();
    char *content = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    Value v;
    for(auto const& column_name: this->column_names)
    {
	ColumnAttribute ca = this->column_attributes[col_num++];
        if(ca.get_data_type() == ColumnAttribute::DataType::INT)
	{
	    int32_t n = *(int32_t*)(content + offset);
	    offset += sizeof(int32_t);
	    v = Value(n);
	    
	}
	else if(ca.get_data_type() == ColumnAttribute::DataType::TEXT)
	{
	    u16 size = *(u16*)(content + offset);
	    offset += sizeof(u16);
	    v = Value(std::string(content + offset, size));
	    offset += size;
	}
	else
	{
	    throw DbRelationError("Only know how to unmarshal INT and TEXT");
	}
	row->insert(std::pair<Identifier, Value>(column_name, v));
    }
    return row;
}

//provaided in milestone2 page on canvas
Handles* HeapTable::select()
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for(auto const& block_id: *block_ids)
    {
	SlottedPage *block = file.get(block_id);
	RecordIDs *record_ids = block->ids();
	for(auto const& record_id: *record_ids) handles->push_back(Handle(block_id, record_id));
	delete record_ids;
	delete block;
    }
    delete block_ids;
    return handles;
}

//ignore where criteria for select for this sprint
Handles* HeapTable::select(const ValueDict *where)
{
    throw DbRelationError("Function Not Implemented: select with where clause is not supported currently.");
}

//ignroe in this sprint
void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
    throw DbRelationError("Function Not Implemented: Update is not supported currently.");
}

//ignore in this sprint
void HeapTable::del(const Handle handle)
{
    throw DbRelationError("Function Not Implemented: delet is not supported currently.");
}

ValueDict* HeapTable::project(Handle handle)
{
    this->open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    return unmarshal(data);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    ValueDict* row = this->project(handle);
    if(column_names == nullptr)
    {
	return row;
    }
    ValueDict *result = new ValueDict();
    for(auto const&column_name : *column_names)
    {
	result->insert(std::pair<Identifier, Value>(column_name, (*row)[column_name]));
    }
    return result;
}

Handle HeapTable::append(const ValueDict* row)
{
    Dbt* data = marshal(row);
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID id;
    try
    {
	id = block->add(data);
    }
    catch(...)
    {
	block = this->file.get_new();
	id = block->add(data); 
    }
    this->file.put(block);
    return std::pair<BlockID, RecordID>(this->file.get_last_block_id(), id);
}
