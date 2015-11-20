

#include "db11.h"


db11::result::result()
{
}

db11::result::result( result_t rs )
{
	results = rs;
}

db11::result::result( table &tb )
{
	results = tb._table._data;
}

void db11::result::add_row( row_t row )
{
	results.push_back( row );
}

void db11::result::set_column_names( std::string name, columns_t cols )
{
	_cols = 0;
	_columns.clear();
	for( auto c : cols )
	{
		_columns[name][c.first] = c.second + _cols;
	}
	_cols += cols.size(); 
}

void db11::result::set_column_names( named_columns_t columns )
{
	_cols = 0;
	_columns.clear();
	add_column_names( columns );
}

void db11::result::add_column_names( named_columns_t columns )
{
	for( auto cm : columns )
	{
		for( auto c : cm.second )
		{
			_columns[cm.first][c.first] = c.second + _cols;
		}
		_cols += cm.second.size();
	}

	//_cols = col_count;
}

db11::field_t db11::result::operator()(unsigned int row, std::pair<std::string, std::string> look )
{	
	if( results.size() > row )
	{
		row_t &r = get_row( row );
		int f = field(look.first, look.second);
		if( f >= 0 )
			return r[f];
	}
	
	field_t tmp;
	return tmp;
}

int db11::result::field( std::string fld )
{
	for( auto a : _columns )
	{
		auto b = a.second.find( fld );
		if( b != a.second.end() )
		{
			return b->second;
		}
	}

	return -1;
}

int db11::result::field( std::string name, std::string fld )
{
	auto a = _columns.find( name );
	if( a != _columns.end() )
	{
		auto b = a->second.find( fld );
		if( b != a->second.end() )
		{
			return b->second;
		}
	}

	return -1;
}

unsigned int db11::result::count()
{
	return results.size();
}

db11::row_t& db11::result::get_row( unsigned int x )
{
	return results[x];
}

db11::result db11::result::greater_than( const char* fld, const char* val )
{
	return greater_than( field(fld), val );
}

db11::result db11::result::less_than( const char* fld, const char* val )
{
	return less_than( field(fld), val );
}

db11::result db11::result::greater_than_equal( const char* fld, const char* val )
{
	return greater_than_equal( field(fld), val );
}

db11::result db11::result::less_than_equal( const char* fld, const char* val )
{
	return less_than_equal( field(fld), val );
}

db11::result db11::result::greater_than( int field, const char* value )
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

db11::result db11::result::less_than( int field, const char *value )
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

db11::result db11::result::greater_than_equal( int field, const char * value )
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

db11::result db11::result::less_than_equal( int field, const char* value )
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
		
db11::result& db11::result::order_alpha( std::pair<std::string, std::string> fld )
{	
	int f = field( fld.first, fld.second );
	
	std::sort( results.begin(), results.end(), [=]( row_t a, row_t b ) { return a[f] < b[f]; } );
	
	return *this;
}
		
db11::result& db11::result::order_num( std::pair<std::string, std::string> fld )
{
	int f = field( fld.first, fld.second );
	
	std::sort( results.begin(), results.end(), [=]( row_t a, row_t b) { return atoi( a[f].c_str() ) < atoi( b[f].c_str() ); } );
	
	return *this;
}
		
db11::row_t& db11::result::operator[]( unsigned int x )
{
	return get_row( x );
}
