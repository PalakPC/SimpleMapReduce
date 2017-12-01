#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
 * This is a big task for this project, will test your understanding
 * of map reduce
 */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions
		 * as per the need of your implementation
		 */

};

extern std::shared_ptr<BaseMapper>
get_mapper_from_task_factory(const std::string& user_id);

extern std::shared_ptr<BaseReducer>
get_reducer_from_task_factory(const std::string& user_id);
