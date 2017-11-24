#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
 * This is probably the biggest task for this project, will test your
 * understanding of map reduce.
 */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		std::list<struct worker> worker_queue;
		std::vector<FileShard> shards;
};


/* CS6210_TASK: This is all the information your master will
 * get from the framework. You can populate your other class data members
 * here if you want.
 */
Master::Master(const MapReduceSpec& mr_spec,
	       const std::vector<FileShard>& file_shards) {

	worker_queue = mr_spec.worker_queue;
	shards = file_shards;
}


/* CS6210_TASK: Here you go. once this function is called you will complete
 * whole map reduce task and return true if succeeded.
 */
bool Master::run() {

	int num_completed_map_tasks = 0;

	while (num_completed_map_tasks < shards.size()) {

		while (!worker_queue.empty()) {
			
			/* Assign Workers to Map Tasks */
		}

		while (worker_queue.empty()) {
			
			/* Receive Completed Requests. Update Invariants */
		}
	}
	return true;
}
