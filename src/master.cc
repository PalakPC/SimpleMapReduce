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
			new WorkerRpc(&cq, grpc::CreateChannel(
					      mr_spec.addrs.at(ii),
					      grpc::InsecureChannelCredentials()));
		workers.push_back(worker);
		worker_queue.push_back(worker);
		
	}

	/* Set up fresh batch of map requests to be submitted to workers. */
	for (int ii = 0; ii < file_shards.size(); ii++) {
		FileShard cur = file_shards.at(ii);
		new_map_requests.push_back(&cur.shards);
	}

}

std::chrono::system_clock::time_point tick(unsigned timeout) {
	return std::chrono::system_clock::now() +
		std::chrono::milliseconds(timeout);
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

		/* if all workers are assigned to map tasks, block until at least
		 * one is complete. Hopefully, all outstanding requests complete
		 * within a single iteration and we recover a full set of workers.
		 */
		if (worker_queue.empty()) {

			void *tag;
			bool ok;
			AsyncMapCall *call;

			/* Block until at least one worker is complete. May need to 
			 * put a deadline here if all workers fail. Probably won't
			 * be necessary though for the purposes of this project.
			 */
			GPR_ASSERT(cq.Next(&tag, &ok));
			GPR_ASSERT(ok);

			call = static_cast<AsyncMapCall*>(tag);
			worker_queue.push_back(call->worker);
			pending_map_requests.erase(call->request);

			/* Wait 100ms for all remaining outstanding requests. If not
			 * all arrive continue onward using performant workers.
			 */

			std::chrono::system_clock::time_point deadline = tick(100);
			for (int ii = 0; ii < workers.size() - 1; ii++) {

				GPR_ASSERT(cq.AsyncNext(&tag, &ok, tick(10)));
				if (!ok)
					continue;
				
				call = static_cast<AsyncMapCall*>(tag);
				worker_queue.push_back(call->worker);
				pending_map_requests.erase(call->request);
			}
		}
	}

	/* You should provide a means to reap the stranglers as you
	 * manage reduce tasks. Could make it another thread but that will force
	 * probably force you to use a lock primitive??
	 */
	if (workers.size() != worker_queue.size()) {
		std::cout << "Insert a Reap Method Here!!!" << std::endl;
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
