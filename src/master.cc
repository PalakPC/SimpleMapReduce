#include "master.h"

/* CS6210_TASK: This is all the information your master will
 * get from the framework. You can populate your other class data members
 * here if you want.
 */
Master::Master(const MapReduceSpec& mr_spec,
	       const std::vector<FileShard>& file_shards) {

	/* Create the woker queue */
	for (int ii = 0; ii < mr_spec.addrs.size(); ii++) {

		WorkerRpc *worker =
			new WorkerRpc(grpc::CreateChannel(
					      mr_spec.addrs.at(ii),
					      grpc::InsecureChannelCredentials()));
		mapper_queue.push_back(worker);
	}

	total_workers = mapper_queue.size();
	/* Set up fresh batch of map requests to be submitted to workers. */
	for (int ii = 0; ii < file_shards.size(); ii++) {
		FileShard cur = file_shards.at(ii);
		new_map_requests.push_back(&cur.shards);
	}

}

inline std::chrono::system_clock::time_point Master::tick(unsigned timeout) {
	return std::chrono::system_clock::now() +
		std::chrono::milliseconds(timeout);
}


/* Collect stranglers from map task and add them to reducer queue */
inline void Master::reap(unsigned wait) {

	if (mapper_queue.size() != total_workers) {

		AsyncMapCall *call;
		std::chrono::system_clock::time_point deadline = tick(wait);
		
		for (int ii = mapper_queue.size(); ii < total_workers; ii++) {

			call = WorkerRpc::recvMapResponseAsync(deadline);
			if (!call)
				return;

			reducer_queue.push_back(call->worker);
		}
	}
}


bool Master::manageMapTasks() {

	WorkerRpc *cur;
	MapRequest *req;

	/* While there are pending and new map request, submit map requests
	 * to workers.
	 */
	while (!pending_map_requests.empty() && !new_map_requests.empty()) {

		/* First ensure free workers tackle new requests and hope the
		 * stranglers complete when we collect completed map responses.
		 */
		while (!mapper_queue.empty() && !new_map_requests.empty()) {

			cur = mapper_queue.front();
			mapper_queue.pop_front();

			req = new_map_requests.front();
			new_map_requests.pop_front();

			cur->sendMapRequest(req);
			pending_map_requests.insert(req);
		}

		/* Reassign free workers iff all new map requests have been
		 * processed. We should no longer hope for stranglers to complete.
		 */
		if (!mapper_queue.empty() && !pending_map_requests.empty()) {

			std::set<MapRequest*>::iterator iter =
				pending_map_requests.begin();

			while (!mapper_queue.empty() &&
			       iter != pending_map_requests.end()) {
				
				cur = mapper_queue.front();
				mapper_queue.pop_front();

				cur->sendMapRequest(*iter);
				iter++;
			}
		}

		/* if all workers are assigned to map tasks, block until at least
		 * one is complete. Hopefully, all outstanding requests complete
		 * within a single iteration and we recover a full set of workers.
		 */
		if (mapper_queue.empty()) {


			/* Block until one mapper response arrives */
			AsyncMapCall *call = WorkerRpc::recvMapResponseSync();
			mapper_queue.push_back(call->worker);
			pending_map_requests.erase(call->request);

			/* Wait 100ms for all remaining outstanding requests. If not
			 * all arrive continue onward using performant workers.
			 */
			std::chrono::system_clock::time_point deadline = tick(100);
			for (int ii = 0; ii < total_workers - 1; ii++) {

				call = WorkerRpc::recvMapResponseAsync(deadline);

				/* Check for elapsed deadline */
				if (!(call))
					break;

				mapper_queue.push_back(call->worker);
				pending_map_requests.erase(call->request);
			}
		}
	}

	 /* Attempt to wait and collect stranglers placing them in reducer queue */
	reap(100);

	/* Set up reducer queue with all idle workers not collected in reap() */
	for (std::list<WorkerRpc*>::iterator iter = mapper_queue.begin();
	     iter != mapper_queue.end(); iter++) {
		reducer_queue.push_back(*iter);
	}
	return true;
}


bool Master::manageReduceTasks() {
	return true;
}


/* CS621_TASK: Here you go. once this function is called you will complete
 * whole map reduce task and return true if succeeded.
 */
bool Master::run() {
	return manageMapTasks() && manageReduceTasks();
}
