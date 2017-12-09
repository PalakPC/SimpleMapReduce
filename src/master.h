#pragma once

#include "worker_rpc.h"
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <list>
#include <cstdio>

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
 * This is probably the biggest task for this project, will test your
 * understanding of map reduce.
 */
class Master {
      
private:
	unsigned total_workers;
	std::string output_path;

	/* Data Structures for managing mappers */
	std::list<WorkerRpc*> mapper_queue;
	std::list<MapRequest*> new_map_requests;
	std::unordered_set<MapRequest*> pending_map_requests;

	/* Data Structures for managing reducers */
	std::list<WorkerRpc*> reducer_queue;
	std::vector<ReduceRequest> reduce_requests;
	std::list<ReduceRequest*> new_reduce_requests;
	std::unordered_set<ReduceRequest*> pending_reduce_requests;
	
	/* Privte Memeber Functions */
	void updateReduceRequests(AsyncMapCall *call);
	void processReducerOutput(AsyncReduceCall *call);
	bool manageMapTasks(void);
	bool manageReduceTasks(void);

	inline std::chrono::system_clock::time_point tick(unsigned wait_time);
	inline std::string genOutFile(std::string key);
	inline void reap_old_map_requests(unsigned wait_time);

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec&, const std::vector<FileShard>&);

	/* DON'T change this function's signature */
	bool run();
};
