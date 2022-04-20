#include "heap_storage.h"

typedef u_int16_t u16;

bool test_heap_storage() {return true;}

// Slotted Page
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new = false)
{
  if (is_new)
  {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  } else{
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt* data)
{
  if(!has_room(data->get_size()))
    throw DbBlockNoRoomError("Not enough room for new record");
  u16 id = ++this->num_records;
  u16 size = (u16) data->get_size();
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcopy(this->address(loc), data->get_data(), size);
  return id;
}

Dbt* SlottedPage::get(RecordID record_id)
{
  u16 size = 
}

RecordIDs* SlottedPage::ids(void)
{
  RecordIDs* ids = new RecordIDs;
  for (u16 id = 1; id <= this->num_records; id++)
  {
    // ensures that record is not tombstoned
    if(get_n(4 * id) != 0)
      ids->push_back(id);
  } 
  return ids;

// Get 2-byte integer at given offset in block
u16 SlottedPage::get_n(u16 offset)
{
  return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block
void SlottedPage::put_n(u16 offset, u16 n)
{
  *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block
void* SlottedPage::address(u16 offset)
{
  return (void*)((char*)this->block.get_data() + offset);
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0)
{
  size = get_n(4 * id);
  loc = get_n(4 * id + 2);
}

bool SlottedPage::has_room(u_int16_t size)
{
  u16 availableSpace = this->end_free - (this->num_records + 1) * 4;
  return availableSpace >= size;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
  u16 shift = end - start;

}

// Store the size and offset for given id. For id of zero, store the block header
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
  if (id == 0)
  { 
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(4*id, size);
  put_n(4*id + 2, loc);
}

