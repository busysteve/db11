
#include "db11.h"
#include <iostream>

using namespace std;

int main()
{
	
	db11::table  table;
	typedef db11::row_t row;

	
	table.insert_row( row{ "Hello", "You", "stank", "fool" }, 2);
	table.insert_row( row{ "So", "long", "sucka", "tash" }, 2);
	table.insert_row( row{ "Bruce", "Lee", "kung", "fu" }, 2);
	table.insert_row( row{ "Crazy", "Ja", "big", "head" }, 2);
	table.insert_row( row{ "Bruce", "Lee", "Game", "Death" }, 2);

	typedef db11::idx_t key;
	db11::id_t id;

	id = table.get_row_id( key{"Bruce", "Lee"} );

	if( id > 0 )
	{
		db11::result rs = table[id];
		cout << rs.count() << endl;
		for( db11::id_t i=0; i < rs.count(); i++ )
			cout << rs[i][2] << endl << flush;
	}
	
	cout << endl;

	table.store( "data_out.dat" );
	
	table.load( "data.dat", 2 );

	id = table.get_row_id( key{"Cool", "Dude"} );

	if( id > 0 )
	{
		db11::result rs = table[id];
		cout << rs.count() << endl;
		for( db11::id_t i=0; i < rs.count(); i++ )
			cout << rs[i][2] << endl << flush;
	}

	cout << endl;

	id = table.get_row_id( key{"Bruce", "Lee"} );

	if( id > 0 )
	{
		db11::result rs = table[id];
		cout << rs.count() << endl;
		for( db11::id_t i=0; i < rs.count(); i++ )
			cout << rs[i][2] << endl << flush;
	}

	return 0;
}


