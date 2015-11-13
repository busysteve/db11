
#ifndef __DB11
#define __DB11

#include <map>
#include <unordered_map>
#include <tuple>
#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <sstream>
#include <utility>
#include <iostream>
#include <cstdlib>

using namespace std;


class db11
{
public:
	typedef std::string                  field_t;
	typedef unsigned long                id_t;
	typedef std::vector<field_t>         row_t;
	typedef row_t                        fields_t;
	typedef std::multimap<id_t,row_t>    table_t;
	typedef std::map< field_t, table_t > tables_t;
	typedef std::vector<row_t>           result_t;
	typedef std::vector<field_t>         idx_t;
	typedef std::unordered_map< field_t, int >     columns_t;
	typedef std::map< int, field_t >     r_columns_t;  // Used for ordered reverse lookup on storage
	typedef std::unordered_map< field_t, columns_t >  named_columns_t;
	typedef std::tuple<field_t, field_t, field_t, field_t, field_t, field_t, field_t> lookup_t;

	class result;
	class table;
	class index;
	class inner;
	class left;

	

	class index
	{
		friend              db11;
	public:


		index() : idx_id(0L)
		{}

		id_t lookup_id( lookup_t tup )
		{
			auto it = idx.find( tup );

			if( it == idx.end() )
				return 0L;

			return it->second;
		}

		id_t insert_id( lookup_t tup )
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
		map< lookup_t, id_t > idx;
		atomic<id_t> idx_id;
	};

	class result
	{
		friend table;
		friend inner;

		int _cols{0};

		named_columns_t    _columns;
		
		void add_row( row_t row )
		{
			results.push_back( row );
		}

		void set_column_names( std::string name, columns_t cols )
		{
			_cols = 0;
			_columns.clear();
			for( auto c : cols )
			{
				_columns[name][c.first] = c.second + _cols;
				//cout << "\t" << name << "." << c.first << "\t" << _columns[name][c.first] << endl;
			}
			_cols += cols.size(); 
		}

		void set_column_names( named_columns_t columns )
		{
			_cols = 0;
			_columns.clear();
			add_column_names( columns );
		}

		void add_column_names( named_columns_t columns )
		{
			for( auto cm : columns )
			{
				for( auto c : cm.second )
				{
					_columns[cm.first][c.first] = c.second + _cols;
					//cout << "\t" << cm.first << "." << c.first << "\t" << _columns[cm.first][c.first] << endl;
				}
				_cols += cm.second.size();
			}

			//_cols = col_count;
		}

	public:

		field_t operator()(int row, field_t name, field_t fld )
		{
			return get_row( row )[field(name, fld)];
		}
	
		unsigned int field( std::string fld )
		{
			for( auto a : _columns )
			{
				auto b = a.second.find( fld );
				if( b != a.second.end() )
				{
					//cout << a.first << ":" << b->first << ":" << b->second << endl;
					return b->second;
				}
			}

			return 0;
		}

		unsigned int field( std::string name, std::string fld )
		{
			auto a = _columns.find( name );
			if( a != _columns.end() )
			{
				auto b = a->second.find( fld );
				if( b != a->second.end() )
				{
					//cout << a->first << ":" << b->first << ":" << b->second << endl;
					return b->second;
				}
			}

			return 0;
		}

		unsigned int count()
		{
			return results.size();
		}

		row_t& get_row( unsigned int x )
		{
			return results[x];
		}

		result greater_than( const char* fld, const char* val )
		{
			return greater_than( field(fld), val );
		}

		result less_than( const char* fld, const char* val )
		{
			return less_than( field(fld), val );
		}

		result greater_than_equal( const char* fld, const char* val )
		{
			return greater_than_equal( field(fld), val );
		}

		result less_than_equal( const char* fld, const char* val )
		{
			return less_than_equal( field(fld), val );
		}

		result greater_than( int field, const char* value )
		{
			result rs;
			rs.set_column_names( _columns );
			
			for( auto r : results )
			{
				if( atoi(r[field].c_str()) > atoi(value) )
					rs.add_row( r );
			}
			return rs;
		}

		result less_than( int field, const char *value )
		{
			result rs;
			rs.set_column_names( _columns );
			
			for( auto r : results )
			{
				if( atoi(r[field].c_str()) < atoi(value) )
					rs.add_row( r );
			}
			return rs;
		}

		result greater_than_equal( int field, const char * value )
		{
			result rs;
			rs.set_column_names( _columns );
			
			for( auto r : results )
			{
				if( atoi(r[field].c_str()) >= atoi(value) )
					rs.add_row( r );
			}
			return rs;
		}

		result less_than_equal( int field, const char* value )
		{
			result rs;
			rs.set_column_names( _columns );
			
			for( auto r : results )
			{
				if( atoi(r[field].c_str()) <= atoi(value) )
					rs.add_row( r );
			}
			return rs;
		}
				
		row_t& operator[]( unsigned int x )
		{
			return get_row( x );
		}

	private:
		result_t results;
	};


	class inner
	{
		result &_r1, &_r2;
		std::vector< std::tuple< field_t, int, int > > operations;
	public:
		inner( result& r1, result& r2 ) 
			: _r1(r1), _r2(r2)
		{
		}

		inner& equal( int lv, int rv )
		{
			operations.push_back( make_tuple("eq", lv, rv ) );

			return *this;
		}

		result join()
		{
			result rs;
			
			rs.add_column_names( _r1._columns );
			rs.add_column_names( _r2._columns );

			bool   assign_row_to_result;

			for( auto x : _r1.results )
			{
				for( auto y : _r2.results )
				{
					assign_row_to_result = false;

					for( auto z : operations )
					{
						string& op = std::get<0>(z);
						int     li = std::get<1>(z);
						int     ri = std::get<2>(z);

						if( op == "eq" )
						{
							if( x[li] == y[ri] )
							{
								assign_row_to_result = true;
							}
							else
							{
								assign_row_to_result = false;
								break;
							}
						}
					}

					if( assign_row_to_result )
					{
						row_t  row;

						for( auto a : x )
							row.push_back( a );

						for( auto b : y )
							row.push_back( b );

						rs.add_row( row );
					}
				}
			}

			return rs;
		}
	};


	class table
	{
		friend                   db11;
		columns_t                _fields;
		r_columns_t              _r_fields;
		int                      _cols;
		field_t                  _name;
		id_t			 _ix;

	public:

		table( field_t name, fields_t fields )
		{
			_name = name;

			int c=0;
			for( auto f : fields )
			{
				_fields[f] = c;
				_r_fields[c++] = f;
			}

			_cols = c;
		}
	
		result operator[]( id_t id )
		{
			return get_data(id);
		}

		result operator[]( field_t name )
		{
			return get_data(_fields[name]);
		}

		result get_data( id_t id )
		{
			result rs;

			rs.set_column_names( _name, _fields );

			auto range = _table.equal_range(id);

			for( auto it = range.first; it != range.second; it++ )
				rs.add_row( it->second );

			return rs;
		}

		id_t insert( row_t data, unsigned int ix )
		{
			_ix = ix;
			field_t temp("");

			if( data.size() < ix )
				return 0L;

			auto tup = std::make_tuple( 
					(ix > 0) ? data[0] : temp, 
					(ix > 1) ? data[1] : temp, 
					(ix > 2) ? data[2] : temp, 
					(ix > 3) ? data[3] : temp, 
					(ix > 4) ? data[4] : temp, 
					(ix > 5) ? data[5] : temp, 
					(ix > 6) ? data[6] : temp  );

			id_t id = idx.lookup_id( tup );

			if( id == 0L )
				id = idx.insert_id( tup );

			_table.insert( std::make_pair( id, data) );

			return id;
		}

		id_t get_id( idx_t key )
		{
			int ix = key.size();
			field_t temp("");

			auto tup = std::make_tuple( 
					(ix > 0) ? key[0] : temp, 
					(ix > 1) ? key[1] : temp, 
					(ix > 2) ? key[2] : temp, 
					(ix > 3) ? key[3] : temp, 
					(ix > 4) ? key[4] : temp, 
					(ix > 5) ? key[5] : temp, 
					(ix > 6) ? key[6] : temp  );
			id_t id = idx.lookup_id( tup );

			return id;
		}

		void store( ofstream& ofs )
		{
			for( auto row : _table )
			{
				string str;

				for( auto field : row.second )
				{
					str += field + '\t';
				}

				str[str.length()-1] = '\n';

				ofs << str;
			}

			// "End-of-table" data-line
			ofs << "@\n" << flush;
		}

		void load( std::ifstream &ifs, int ix )
		{
			string line;
			std::istringstream iss;
			row_t row;

			while( std::getline( ifs, line, '\n' ) )
			{
				// "End-of-table" data-line
				if( line[0] == '@' )
				{
					return;
				}

				row.clear();
				iss.str( line );
				field_t word;
				while( std::getline( iss, word, '\t' ) )
				{
					row.push_back(word);
				}

				if( row.size() > (size_t)(ix) )
				{
					insert( row, ix );
				}

				iss.clear();		
			}
		}

		void clear()
		{
			idx.clear();
			_table.clear();
		}


	private:
		table_t _table;
		index idx;
	};

public:
	
	void create_table( field_t name, fields_t columns )
	{
		_tables[name] = new table( name, columns );
	}
	
	table& operator[]( field_t name )
	{
		return *(_tables[name]);
	}

	void store( const char * filename )
	{
		ofstream ofs ( filename, std::ofstream::binary );

		for( auto t : _tables )
		{
			if( t.first.length() > 0 )
			{
				// Write table name
				ofs << '~' << t.first;

				ofs << '~' << t.second->_ix;

				for( auto cols : t.second->_r_fields )
					ofs << '~' << cols.second;

				ofs << '\n';

				t.second->store( ofs );				
			}

			ofs << flush;
		}
	}

	void load( const char * filename )
	{
		string line;

		_tables.clear();

		std::ifstream ifs ( filename, std::ifstream::binary);
		std::istringstream iss;
		row_t row;

		while( std::getline( ifs, line, '\n' ) )
		{
			// Create table from file
			if( line[0] == '~' )
			{
				iss.clear();			
		
				iss.str( line.substr(1) );
				fields_t flds;
				field_t  name;
				field_t  ix;
				field_t  column;

				// Get table name
				std::getline( iss, name, '~' );

				// Get index count
				std::getline( iss, ix, '~' );

				// Get column names
				while( std::getline( iss, column, '~' ) )
				{
					flds.push_back( column );
				}
				
				// Create table
				create_table( name, flds );

				// Load in table data
				_tables[name]->load( ifs, atoi(ix.c_str()) );
			}
		}
	}
	
private:
	std::map<std::string, table*>  _tables;
};


#endif

