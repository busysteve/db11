
#include "db11.h"


void db11::create_table( db11::field_t name, db11::fields_t columns )
{
	_tables[name] = new db11::table( name, columns );
}

std::vector<db11::name_t> db11::list_tables()
{
    std::vector<name_t> names;
    for( auto t : _tables )
        names.push_back(t.first);

    return names;
}

db11::table& db11::operator[]( name_t name )
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

			for( auto cols : t.second->_r_fields )
				ofs << '~' << cols.second;

            // Write index specs
            if( !t.second->_idxs.empty() ) {
                for (auto i: t.second->_idxs) {
                    ofs << "\n^" << t.first;
                    for (auto f: i.first)
                        ofs << '~' << f;
                }
            }

            ofs << "\n$" << t.first << "\n";

			t.second->store( ofs );				
		}

        ofs << std::flush;
	}
}

void db11::load( const char * filename )
{
	std::string line;

	_tables.clear();

	std::ifstream ifs ( filename, std::ifstream::binary );
	std::istringstream iss;
	row_t row;

	while( !std::getline( ifs, line, '\n' ).eof() ) {
        std::cout << line << std::endl;
        // Create table from file
        if (line[0] == '~') {
            iss.clear();

            iss.str(line.substr(1));
            fields_t flds;
            field_t fld;
            field_t tbl;
            field_t index;
            field_t spec;
            field_t ix;
            field_t column;

            // Get table name
            std::getline(iss, tbl, '~');

            // Get column names
            while (std::getline(iss, column, '~')) {
                flds.push_back(column);
            }

            // Create table
            db11::create_table(tbl, flds);

            // Load in table data
            //_tables[tbl]->load(ifs);
        } else if (line[0] == '^') {
            std::cout << "building index" << std::endl;
            fields_t flds;
            field_t fld;
            field_t tbl;
            field_t index;
            field_t spec;
            field_t ix;
            field_t column;

            iss.clear();

            iss.str(line.substr(1));

            // Indexing
            //std::getline( iss, index, '|' );
            std::getline(iss, tbl, '~');

            flds.clear();
            while (std::getline(iss, fld, '~')) {
                std::cout << "add " << fld << " to index" << std::endl;
                std::cout << fld << std::endl;
                flds.push_back(fld);
            }

            if (!flds.empty()) {
                std::cout << "create index" << std::endl;
                (*this)[tbl].create_index(flds);
            }

            std::cout << std::flush;
        }
        else if( line[0] == '$' )
        {
            iss.clear();

            std::string tbl(line.substr(1));

            // Load in table data
            _tables[tbl]->load(ifs);
        }
    }
}
