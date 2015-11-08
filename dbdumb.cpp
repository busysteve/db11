
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

	typedef dbdumb_index::idx_t idx;
	dbdumb_table::id_t id;

	id = table.get_row_id( idx{"Bruce", "Lee"} );


	cout << table[id][2] << endl << flush;

	return 0;
}


