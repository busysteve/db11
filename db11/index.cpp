
#include "db11.h"


db11::index::index() : idx_id(1L)
{}

int db11::index::build( db11::fields_t flds, db11::columns_t  cols, db11::table_t  tab )
{
	// Check fields
	
	int count = 0;
	
	for( auto r : tab._data_ref )
	{
		id_t id = r.first;
		
		row_t key;
		
		for( auto f : flds )
		{
			auto c = cols[f];
			key.push_back( tab._data[r.second][c] );
		}
		
		idxs.insert( std::make_pair( key, id ) );
		++count;
	}
	
	return count;
}


db11::ids_t db11::index::lookup_id( db11::lookup_t key )
{
	auto range = idxs.equal_range(key);
		
	ids_t ids;

	for( auto it = range.first; it != range.second; it++ )
	{	
		ids.push_back( it->second );
	}

	return ids;
}

db11::id_t db11::index::insert_id( db11::lookup_t key, db11::id_t id )
{
	//unsigned long id=0L;

	//id = get_next_id();

	idxs.insert( std::make_pair( key, id ) );

	return id;
}


db11::id_t db11::index::get_next_id()
{
	return ++idx_id;
}

void db11::index::clear()
{
	idxs.clear();
}
