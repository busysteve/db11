
#include "dbdumb.h"
#include <iostream>

using namespace std;

int main()
{
	
	dbdumb_table  table;
	typedef dbdumb_table::row_t row;

	
	table.insert_row( row{ "Hello", "You", "stank", "fool" }, 2);
	table.insert_row( row{ "So", "long", "sucka", "tash" }, 2);
	table.insert_row( row{ "Bruce", "Lee", "kung", "fu" }, 2);
	table.insert_row( row{ "Crazy", "Ja", "big", "head" }, 2);

	typedef dbdumb_index::idx_t key;
	dbdumb_table::id_t id;

	id = table.get_row_id( key{"Bruce", "Lee"} );

	cout << table[id][2] << endl << flush;

	table.store( "data_out.dat" );
	
	table.load( "data.dat", 2 );

	id = table.get_row_id( key{"Cool", "Dude"} );

	if( id > 0 )
		cout << table[id][2] << endl << flush;

	id = table.get_row_id( key{"Bruce", "Lee"} );

	if( id > 0 )
		cout << table[id][2] << endl << flush;

	return 0;
}


