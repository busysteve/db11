
#ifndef __DBDUMB
#define __DBDUMB

#include <tuple>
#include <map>
#include <vector>
#include <string>
#include <atomic>

using namespace std;

class dbdumb_index
{
public:
	typedef unsigned long 	id_t;
	typedef vector<string>	idx_t;


	dbdumb_index() : idx_id(0L)
	{}

	id_t lookup_id( tuple<string, string, string, string, string, string> tup )
	{
		auto it = idx.find( tup );

		if( it == idx.end() )
			return 0L;

		return it->second;
	}

	id_t insert_id( tuple<string, string, string, string, string, string> tup )
	{
		unsigned long id=0L;

		id = get_next_id();

		idx[ tup ] = id;

		return id;
	}

	id_t get_next_id()
	{
		return ++idx_id;
	}

private:
	map< tuple<string, string, string, string, string, string>, unsigned long > idx;
	atomic<unsigned long> idx_id;
};

class dbdumb_table
{
public:

	typedef dbdumb_index::id_t id_t;
	typedef vector<string>    row_t;
	

	string get_data( id_t id, int offset )
	{
		return table[id][offset];
	}

	void push_data( id_t id, string data )
	{
		table[id].push_back( data );
	}

	vector<string>& operator[]( id_t id )
	{
		return get_row(id);
	}

	vector<string>& get_row( id_t id )
	{
		return table[id];
	}

	id_t insert_row( row_t data, int ix = 0 )
	{
		string temp("");

		auto tup = make_tuple( 
					 data[0], 
				(ix > 1) ? data[1] : temp, 
				(ix > 2) ? data[2] : temp, 
				(ix > 3) ? data[3] : temp, 
				(ix > 4) ? data[4] : temp, 
				(ix > 5) ? data[5] : temp  );
		id_t id = idx.insert_id( tup );
		table[id] = data;
		return id;
	}

	id_t get_row_id( dbdumb_index::idx_t key )
	{
		int ix = key.size();
		string temp("");

		auto tup = make_tuple( 
					 key[0], 
				(ix > 1) ? key[1] : temp, 
				(ix > 2) ? key[2] : temp, 
				(ix > 3) ? key[3] : temp, 
				(ix > 4) ? key[4] : temp, 
				(ix > 5) ? key[5] : temp  );
		id_t id = idx.lookup_id( tup );

		return id;
	}
private:
	map< id_t, vector<string> > table;
	dbdumb_index idx;
};




#endif
