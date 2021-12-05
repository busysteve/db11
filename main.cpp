
#include "db11/db11.h"
#include <iostream>

using namespace std;

void dump_results( db11::result &rs )
{
	cout << "row count = " << rs.count() << endl;
	for( db11::id_t r=0; r < rs.count(); r++ )
	{
		for( size_t f=0; f < rs[r].size(); f++ )
			cout << rs[r][f] << " " << flush;
		cout << endl;		
	}
	cout << endl;
}

int main()
{

    db11  db;

	typedef db11::fields_t flds;
	typedef db11::columns_t cols;
	typedef db11::idx_t key;
	db11::ids_t ids;

	db.create_table( "authors", flds{ "fname", "lname", "authorid"} );
	db["authors"].create_index( flds{"fname", "lname"} );
	db["authors"].create_index( flds{"authorid"} );
	db["authors"].auto_increment( flds{"authorid"} );
	
	db.create_table( "books", flds{ "title", "year", "bookid", "authorid"} );
	db["books"].create_index( flds{"title"} );
	db["books"].create_index( flds{"year"} );
	db["books"].create_index( flds{"authorid"} );
	db["books"].auto_increment( flds{"bookid"} );
	
	typedef db11::row_t row;

	db["authors"].insert( flds{"fname", "lname"}, row{ "Judas", "Priest" } );
	db["authors"].insert( flds{"fname", "lname"}, row{ "Bruce", "Lee" } );
	db["authors"].insert( flds{"fname", "lname"}, row{ "Frank", "Enstein" } );
	db["authors"].insert( flds{"fname", "lname"}, row{ "Tom", "Petty" } );
	
	// Retrieve Frank Enstein's ID
	ids = db["authors"].get_ids( flds{"fname", "lname"}, key{"Frank", "Enstein"} );
	std::cout << "Author return count = " << ids.size() << std::endl;
	db11::result r1 = db["authors"][ids];
	dump_results( r1 );
	
	// Insert Books with Frank's ID
	if( r1.count() > 0 )
	{
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "Live Again", "1893", r1(0, db11::field("authors", "authorid") ) } );
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "Dead or Alive", "1897", r1(0, db11::field("authors", "authorid") ) } );
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "All in One", "1913", r1(0, db11::field("authors", "authorid") ) } );
	}

	// Retrieve Bruce Lee's ID
	ids = db["authors"].get_ids( flds{"fname", "lname"}, key{"Bruce", "Lee"} );
	std::cout << "Author return count = " << ids.size() << std::endl;
	r1 = db["authors"][ids];
	dump_results( r1 );
	
	// Insert Books with Bruce Lee's ID
	if( r1.count() > 0 )
	{
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "Death Games", "1973", r1(0, db11::field("authors", "authorid") ) } );
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "Gung Fu for Wimps", "1966", r1(0, db11::field("authors", "authorid") ) } );
		db["books"].insert( flds{"title", "year", "authorid"}, row{ "Kicking Butt", "1969", r1(0, db11::field("authors", "authorid") ) } );
	}


	// Get data on Dead or Alive from books
	ids = db["books"].get_ids( flds{"title"}, key{"Dead or Alive"} );
	std::cout << "Books return count = " << ids.size() << std::endl;
	db11::result r2 = db["books"][ids]; 
	dump_results( r2 );


	// Retrieve Dead or Alive's Author by ID from authors
	ids = db["authors"].get_ids( flds{"authorid"}, key{ r2( 0, db11::field("books", "authorid") ) } );
	std::cout << "Author return count = " << ids.size() << std::endl;
	db11::result r3 = db["authors"][ids];
	dump_results( r3 );


	// Join authors to get the author of Dead of Alive
	db11::inner ijn( r2, r3 );
	ijn.equal( db11::field("books", "authorid"), db11::field("authors", "authorid") );
	db11::result r4 = ijn.join();
	dump_results( r4 ); 
	
	// Select all books from books table and display them
	db11::result r5( db["books"] );
	dump_results( r5 ); 
	
	
    std::cout << "storing books_data.db11" << std::endl;
	db.store( "books_data.db11" );

/*
    db11  d2;

    std::cout << "loading books_data.db11" << std::endl;
    d2.load( "books_data.db11" );

    std::cout << "storing books_data2.db11" << std::endl;
    d2.store( "books_data2.db11" );


    db11  d3;

    std::cout << "loading books_data2.db11" << std::endl;
    d3.load( "books_data2.db11" );

    std::cout << "storing books_data3.db11" << std::endl;
    d3.store( "books_data3.db11" );
*/

    db11 nw11;

    nw11.load("northwind.db11");



    return 0;
}



