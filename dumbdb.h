
#ifndef __DUMBDB
#define __DUMBDB

#include <map>
#include <tuple>
#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <sstream>
#include <utility>
#include <iostream>

using namespace std;


class dumbdb
{
public:
	typedef unsigned long 			id_t;
	typedef vector<string>    		row_t;
	typedef multimap< id_t, row_t >	 	table_t;
	typedef vector< row_t > 		result_t;
	typedef row_t				idx_t;

	class result;
	class table;
	class index;


	class index
	{
	public:


		index() : idx_id(0L)
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

		void clear()
		{
			idx.clear();
		}

	private:
		map< tuple<string, string, string, string, string, string>, unsigned long > idx;
		atomic<unsigned long> idx_id;
	};


	class result
	{
		friend table;

		void add_row( dumbdb::row_t row )
		{
			results.push_back( row );
		}

	public:
		typedef vector< dumbdb::row_t > result_t;

		unsigned int count()
		{
			return results.size();
		}

		row_t& get_row( unsigned int x )
		{
			return results[x];
		}

		row_t& operator[]( unsigned int x )
		{
			return get_row( x );
		}

	private:
		result_t results;
	};


	class table
	{
	public:

		result operator[]( id_t id )
		{
			return get_row(id);
		}

		result get_row( id_t id )
		{
			result rs;
			auto range = table.equal_range(id);

			for( auto it = range.first; it != range.second; it++ )
				rs.add_row( it->second );

			return rs;
		}

		id_t insert_row( row_t data, unsigned int ix )
		{
			string temp("");

			if( data.size() < ix )
				return 0L;

			auto tup = make_tuple( 
					(ix > 0) ? data[0] : temp, 
					(ix > 1) ? data[1] : temp, 
					(ix > 2) ? data[2] : temp, 
					(ix > 3) ? data[3] : temp, 
					(ix > 4) ? data[4] : temp, 
					(ix > 5) ? data[5] : temp  );

			id_t id = idx.lookup_id( tup );

			if( id == 0L )
				id = idx.insert_id( tup );

			table.insert( make_pair( id, data) );

			return id;
		}

		id_t get_row_id( idx_t key )
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

		void store( const char * filename )
		{
			ofstream ofs ( filename, std::ofstream::binary );

			for( auto row : table )
			{
				string str;

				for( auto field : row.second )
				{
					str += field + '~';
				}

				str[str.length()-1] = '\n';

				ofs << str;
			}

			ofs << flush;
		
		}

		void load( const char * filename, unsigned int ix )
		{
			string line;

			clear();

			ifstream ifs ( filename, std::ifstream::binary);
			istringstream iss;
			row_t row;

			while( getline( ifs, line, '\n' ) )
			{
				row.clear();
				iss.str( line );
				string word;
				while( getline( iss, word, '~' ) )
				{
					row.push_back(word);
				}

				iss.clear();		

				if( row.size() > ix )
				{
					insert_row( row, ix );
				}
			}
		}

		void clear()
		{
			idx.clear();
			table.clear();
		}


	private:
		table_t table;
		index idx;
	};

};


#endif
