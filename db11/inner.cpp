

#include "db11.h"



db11::inner::inner( result& r1, result& r2 ) 
	: _r1(r1), _r2(r2)
{
}

db11::inner& db11::inner::equal( std::string lv, std::string rv )
{

	operations.push_back( std::make_tuple("eq", _r1._columns[lv], _r2._columns[rv] ) );

	return *this;
}

db11::inner& db11::inner::equal( int lv, int rv )
{
	operations.push_back( std::make_tuple("eq", lv, rv ) );

	return *this;
}

db11::result db11::inner::join()
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
				std::string& op = std::get<0>(z);
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
