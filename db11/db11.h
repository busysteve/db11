
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


class db11
{
public:
	typedef std::string                  field_t;
	typedef unsigned long                id_t;
	typedef std::vector<id_t>            ids_t;
	typedef std::vector<field_t>         row_t;
	typedef row_t                        fields_t;
	typedef std::multimap<id_t,row_t>    table_t;
	typedef std::map< field_t, table_t > tables_t;
	typedef std::vector<row_t>           result_t;
	typedef std::vector<field_t>         idx_t;
	typedef std::unordered_map< field_t, int >     columns_t;
	typedef std::map< int, field_t >     r_columns_t;  // Used for ordered reverse lookup on storage
	typedef std::unordered_map< field_t, columns_t >  named_columns_t;
	//typedef std::tuple<field_t, field_t, field_t, field_t, field_t, field_t, field_t> lookup_t;
	typedef std::vector<field_t>         lookup_t;

	class result;
	class table;
	class index;
	class inner;
	class left;

	
	class index
	{
		friend              db11;
		friend              table;
	public:
		index();
		int build( fields_t flds, columns_t cols, table_t tab );
		ids_t lookup_id( lookup_t key );
		id_t insert_id( lookup_t key, id_t id );
		id_t get_next_id();
		void clear();

	private:
		std::multimap< lookup_t, id_t > idxs;
		std::atomic<id_t> idx_id;
	};

	class contraint
	{
		//vector<index>
	};
	
	class result
	{
		friend table;
		friend inner;

		int _cols{0};

		named_columns_t    _columns;
		
		void add_row( row_t row );
		void set_column_names( std::string name, columns_t cols );
		void set_column_names( named_columns_t columns );
		void add_column_names( named_columns_t columns );

	public:

		field_t operator()(int row, field_t name, field_t fld );
		unsigned int field( std::string fld );
		unsigned int field( std::string name, std::string fld );
		unsigned int count();
		row_t& get_row( unsigned int x );
		result greater_than( const char* fld, const char* val );
		result less_than( const char* fld, const char* val );
		result greater_than_equal( const char* fld, const char* val );
		result less_than_equal( const char* fld, const char* val );
		result greater_than( int field, const char* value );
		result less_than( int field, const char *value );
		result greater_than_equal( int field, const char * value );
		result less_than_equal( int field, const char* value );
		row_t& operator[]( unsigned int x );

	private:
		result_t results;
	};


	class inner
	{
		result &_r1, &_r2;
		std::vector< std::tuple< field_t, int, int > > operations;
	public:
		inner( result& r1, result& r2 );
		inner& equal( std::string lv, std::string rv );
		inner& equal( int lv, int rv );
		result join();
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

		table( field_t name, fields_t fields );
		
		void create_index( fields_t flds );
		result operator[]( ids_t &ids );
		//result operator[]( field_t name );
		result get_data( ids_t &ids );
		id_t insert( row_t data  );
		id_t insert( columns_t flds, row_t data  );
		ids_t get_ids( fields_t flds, idx_t key );
		void store( std::ofstream& ofs );
		void load( std::ifstream &ifs, int ix );
		void clear();


	private:
		table_t _table;
		std::map< lookup_t, index* >  _idxs;
		id_t _id;
	};

public:
	
	void create_table( field_t name, fields_t columns );
	table& operator[]( field_t name );
	void store( const char * filename );
	void load( const char * filename );
	
private:
	std::map<std::string, table*>  _tables;
};


#endif

