#pragma once

#include <mr_task_factory.h>
#include "worker_rpc.h"
#include "mr_tasks.h"
#include "flusher.h"

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
 * This is a big task for this project, will test your understanding
 * of map reduce
 */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		~Worker() {
			server->Shutdown();
			queue->Shutdown();
		}
		/* DON'T change this function's signature */
		bool run();

	private:
		unsigned num_mappings_processed;
		std::string addr_port;
		ServerBuilder builder;
		MapperReducer::AsyncService service;
		std::unique_ptr<ServerCompletionQueue> queue;
		std::unique_ptr<Server> server;
		MapCallData *mcall;
		ReduceCallData *rcall;
	        bool recvMapRequest(void);
		void processMapRequest(void);
		std::string genUniqueFile(MapRequest *req, int rid);
};

extern std::shared_ptr<BaseMapper>
get_mapper_from_task_factory(const std::string& user_id);

extern std::shared_ptr<BaseReducer>
get_reducer_from_task_factory(const std::string& user_id);
