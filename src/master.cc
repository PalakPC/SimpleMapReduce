#include "master.h"

/* CS6210_TASK: This is all the information your master will
 * get from the framework. You can populate your other class data members
 * here if you want.
 */
Master::Master(const MapReduceSpec& mr_spec,
	       const std::vector<FileShard>& file_shards) {

	/* Create the woker queue */
	for (int ii = 0; ii < mr_spec.addrs.size(); ii++) {

		worker_queue.push_back(
			new WorkerRpc(&cq, grpc::CreateChannel(
					      mr_spec.addrs.at(ii),
					      grpc::InsecureChannelCredentials())));
	}

	/* Set up fresh batch of map requests to be submitted to workers. */
	for (int ii = 0; ii < file_shards.size(); ii++) {
		FileShard cur = file_shards.at(ii);
		new_map_requests.push_back(&cur.shards);
	}

}

bool Master::manageMapTasks() {

	WorkerRpc *cur;
	MapRequest *req;
	int completed_map_tasks = 0;
	int num_map_tasks = new_map_requests.size();

	while (completed_map_tasks < num_map_tasks) {

		/* First ensure workers tackle new requests and hope the
		 * stranglers complete when we collect completed map responses.
		 */
		while (!worker_queue.empty() && !new_map_requests.empty()) {

			cur = worker_queue.front();
			worker_queue.pop_front();

			req = new_map_requests.front();
			new_map_requests.pop_front();

			cur->sendMapRequest(req);
			pending_map_requests.insert(req);
		}

		/* Reassign free workers iff all new map requests have been
		 * processed. We should no longer hope for stranglers to complete.
		 */
		if (!worker_queue.empty() && !pending_map_requests.empty()) {

			std::set<MapRequest*>::iterator it =
				pending_map_requests.begin();

			while (!worker_queue.empty() &&
			       it != pending_map_requests.end()) {
				
				cur = worker_queue.front();
				worker_queue.pop_front();

				cur->sendMapRequest(*it);
				it++;
			}
		}

		/* While all workers are assigned to map tasks, spin until at least
		 * one is complete. Hopefully, all outstanding requests complete
		 * within a single iteration and we recover a full set of workers.
		 */
		while (worker_queue.empty()) {

		}

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
