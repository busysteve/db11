
#include "db11.h"


db11::table::table( field_t name, fields_t fields )
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

db11::result db11::table::operator[]( ids_t &ids )
{
	return get_data(ids);
}

db11::ids_t db11::table::get_ids( lookup_t flds, idx_t key )
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	// TODO: check for pointer

	auto pix = _idxs[flds];
	ids_t ids;

	if( pix != nullptr )
	{
		ids = pix->lookup_id( key );
	}
	//else std::cout << "No usable index found" << std::endl;
	

	return ids;
}

/*
result operator[]( field_t name )
{
	return get_data(_fields[name]);
}
*/

db11::result db11::table::get_data( db11::ids_t &ids )
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	result rs;

	rs.set_column_names( _name, _fields );

	for( auto id : ids )
	{
		auto range = _table.equal_range(id);

		for( auto it = range.first; it != range.second; it++ )
			rs.add_row( _table._data[it->second] );
	}
	
	return rs;
}

db11::id_t db11::table::insert( db11::row_t data  )
{
	return db11::table::insert( _fields, data );
}

db11::id_t db11::table::insert( fields_t flds, db11::row_t data  )
{
	columns_t cols;

	int i = 0;
	for( auto f : flds )
	{
		auto c = _fields.find( f );
	
		if( c != _fields.end() )
		{
			cols[c->first] = i++;
		}
		else
		{
			return 0;
		}
	}

	return db11::table::insert( cols, data );
}

db11::id_t db11::table::insert( db11::columns_t flds, db11::row_t data  )
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	field_t temp("");

	if( data.size() != flds.size() )
		return 0L;

	ids_t ids;
	row_t row;

	for( unsigned int i = 0; i < _r_fields.size(); i++  )
	{
		auto f = _r_fields[i];

		auto fi = flds.find( f );

		if( fi != flds.end() )
		{
			row.push_back(data[fi->second]);
			
			auto a = _auto_inc.find( f );

			if( a != _auto_inc.end() )
			{
				int t = atoi( data[fi->second].c_str() );

				if( t > a->second )
					a->second = t;
			}
		}
		else
		{
			auto a = _auto_inc.find( f );

			if( a != _auto_inc.end() )
			{
				a->second++;
				char auto_num[20];
				sprintf( auto_num, "%d", a->second );
				row.push_back(auto_num);
			}
			else
			{
				row.push_back(temp);
			}
		}
	}
	
		
	for( auto ix : _idxs )
	{
		lookup_t key;

		bool key_match = false;

		for( auto i : ix.first )
		{
			auto f = _fields.find( i );

			if( f != flds.end() )
			{
				key.push_back( row[ f->second ] );
				key_match = true;
			}
			else
			{
				key_match = false;
				break;
			}
		}

		if( key_match )
		{
			ids = ix.second->lookup_id( key );
		
			if( ids.size() != 0L ) 
				; // Not unique
		
			ix.second->insert_id( key, _id );
		}
	}


	_table.insert( std::make_pair( _id, row ) );

	return _id++;
}

void db11::table::create_index( db11::fields_t flds )
{
	_idxs[flds] = new index();
	_idxs[flds]->build(flds, _fields, _table);
}

void db11::table::auto_increment( db11::fields_t flds )
{
	for( auto f : flds )
		_auto_inc[f] = 0;
}

void db11::table::store( std::ofstream& ofs )
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	for( auto row : _table )
	{
		std::string str;

		for( auto field : _table._data[row.second] )
		{
			str += field + '\t';
		}

		str[str.length()-1] = '\n';

		ofs << str;
	}

	// "End-of-table" data-line
	ofs << "@\n" << std::flush;
}

void db11::table::load( std::ifstream &ifs )
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	std::string line;
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

		insert( row );

		iss.clear();		
	}
}

void db11::table::clear()
{
	std::lock_guard<std::mutex> lock( _table_mutex );

	_idxs.clear();
	_table.clear();
}
