#pragma once

#include "worker_rpc.h"
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <list>

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
 * This is probably the biggest task for this project, will test your
 * understanding of map reduce.
 */
class Master {
      
private:
	/* Data Structures */
	CompletionQueue cq;
	std::vector<WorkerRpc*> workers;

	/* Data Structures for managing mappers */
	std::list<WorkerRpc*> mapper_queue;
	std::list<MapRequest*> new_map_requests;
	std::set<MapRequest*> pending_map_requests;

	/* Data Structures for managing reducers */
	std::list<WorkerRpc*> reducer_queue;
	std::list<ReduceRequest*> reduce_requests;
	
	/* Privte Memeber Functions */
	bool manageMapTasks(void);
	bool manageReduceTasks(void);
	inline std::chrono::system_clock::time_point tick(unsigned wait_time);
	inline void reap(unsigned wait_time);

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec&, const std::vector<FileShard>&);

	/* DON'T change this function's signature */
	bool run();
};
