#include "heap_storage.h"

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
  }
  this->slide(loc + newSize, loc + size);
  mem_copy()
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
