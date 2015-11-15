
#include "db11.h"


void db11::create_table( db11::field_t name, db11::fields_t columns )
{
	_tables[name] = new db11::table( name, columns );
}

db11::table& db11::operator[]( field_t name )
{
	return *(_tables[name]);
}

void db11::store( const char * filename )
{
	std::ofstream ofs ( filename, std::ofstream::binary );

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

		ofs << std::flush;
	}
}

void db11::load( const char * filename )
{
	std::string line;

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
			db11::create_table( name, flds );

			// Load in table data
			_tables[name]->load( ifs, atoi(ix.c_str()) );
		}
	}
}
