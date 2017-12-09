#pragma once

#include <string>
#include <iostream>

/* CS6210_TASK Implement this data structureas per your implementation.
 * You will need this when your worker is running the map task. Note:
 * structs are identical to classes in C++. The only difference is that
 * structs cannot have private members. So if confused by this code, just
 * think that the get_mapper_from_task_factory() return a regular objectg
 * instance which can perform normal object methods.
 * (i.e. IT's NOT A STRUCT!!!)
 * Then go watch Star Wars Episode 6 or Robot Chicken Star Wars. :)
 */
struct BaseMapperInternal {

	/* DON'T change this function's signature */
	BaseMapperInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* Data structures you can add here. */
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
	
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key,
				     const std::string& val) {
	std::cout << "Dummy emit by BaseMapperInternal: "
		  << key << ", " << val << std::endl;

	/* These keys/values are being parsed in user_task.cc.
	 * We need to associate a key with an intermediate file. Need
	 * to add data structures on base mapper internal to get
	 */
}


/*---------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
 * You will need this when your worker is running the reduce task
 */
struct BaseReducerInternal {

	/* DON'T change this function's signature */
	BaseReducerInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* NOW you can add below, data members and member functions
	 * as per the need of your implementation
	 */
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
	
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key,
				      const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: "
		  << key << ", " << val << std::endl;
}
