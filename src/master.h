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
	CompletionQueue cq;
	std::list<WorkerRpc*> worker_queue;
	std::list<MapRequest*> new_map_requests;
	std::set<MapRequest*> pending_map_requests;
	bool manageMapTasks();
	bool manageReduceTasks();

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec&, const std::vector<FileShard>&);

	/* DON'T change this function's signature */
	bool run();
};
