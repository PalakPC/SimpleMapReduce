#include "master.h"

/* CS6210_TASK: This is all the information your master will
 * get from the framework. You can populate your other class data members
 * here if you want.
 */
Master::Master(const MapReduceSpec& mr_spec,
	       const std::vector<FileShard>& file_shards) :
	total_workers(mr_spec.addrs.size()),
	output_path(mr_spec.outdir),
	map_requests(file_shards.size()),
	reduce_requests(mr_spec.num_reducers) {

	/* Create the woker queue */
	for (unsigned ii = 0; ii < mr_spec.addrs.size(); ii++) {

		WorkerRpc *worker =
			new WorkerRpc(ii, grpc::CreateChannel(
					      mr_spec.addrs.at(ii),
					      grpc::InsecureChannelCredentials()));
		mapper_queue.push_back(worker);
	}

	/* Set up fresh batch of map requests to be submitted to workers. */
	for (int ii = 0; ii < file_shards.size(); ii++) {
		FileShard file_shard = file_shards.at(ii);
		map_requests[ii] = file_shard.mapRequest;
		new_map_requests.push_back(&map_requests[ii]);
	}
}

inline std::chrono::system_clock::time_point Master::tick(unsigned timeout) {
	return std::chrono::system_clock::now() +
		std::chrono::milliseconds(timeout);
}


/* Collect stranglers from map task and add them to reducer queue */
inline void Master::reap_old_map_requests(unsigned wait) {

	if (mapper_queue.size() != total_workers) {

		AsyncMapCall *call;
		std::chrono::system_clock::time_point deadline = tick(wait);
		
		for (int ii = mapper_queue.size(); ii < total_workers; ii++) {

			call = WorkerRpc::recvMapResponseAsync(deadline);
			if (!call) /* Elapsed Deadline */
				return;

			MapReply old_reply = call->reply;
			for (int ii = 0; ii < old_reply.ifiles_size(); ii++) {
				std::string ifile = old_reply.ifiles(ii);
				if (!ifile.empty()) {
					std::remove(ifile.c_str());
				}
			}
			mapper_queue.push_back(call->worker);
			reducer_queue.push_back(call->worker);
			delete call;
		}
	}
}

inline std::string Master::genOutFile(std::string key) {
	return output_path + key + "_output";
}

void Master::updateReduceRequests(AsyncMapCall *call) {

	/* If a map request is still in the pending set, then the results are
	 * brand new. Otherwise, results are old and simply delete the call
	 * along with the produced intermediate files.
	 */
	if (pending_map_requests.find(call->request) !=
	    pending_map_requests.end()) {

		pending_map_requests.erase(call->request);

		MapReply new_reply = call->reply;
		for (unsigned ii = 0; ii < new_reply.ifiles_size(); ii++) {

			ReduceRequest *reduce_req = &reduce_requests[ii];
			std::string ifile = new_reply.ifiles(ii);
			if (!ifile.empty()) {
				std::string *reduce_input =
					reduce_req->add_files();
				reduce_input->assign(ifile);
			}
		}

	} else {

		MapReply discard_reply = call->reply;
		for (unsigned ii = 0; ii < discard_reply.ifiles_size(); ii++) {

			std::string dup_ifile = discard_reply.ifiles(ii);
			if (!dup_ifile.empty()) {
				std::remove(dup_ifile.c_str());
			}
		}
	}

	mapper_queue.push_back(call->worker);
	delete call;
}


bool Master::manageMapTasks() {

	WorkerRpc *mapper;
	MapRequest *req;

	/* While there are pending and new map request, submit map requests
	 * to workers.
	 */
	while (!pending_map_requests.empty() || !new_map_requests.empty()) {

		/* First ensure free workers tackle new requests and hope the
		 * stranglers complete when we collect completed map responses.
		 */
		while (!mapper_queue.empty() && !new_map_requests.empty()) {

			mapper = mapper_queue.front();
			mapper_queue.pop_front();

			req = new_map_requests.front();
			new_map_requests.pop_front();

			pending_map_requests.insert(req);
			mapper->sendMapRequest(req);
		}

		/* Reassign free workers iff all new map requests have been
		 * processed. We should no longer hope for stranglers to complete.
		 */
		if (!mapper_queue.empty() && !pending_map_requests.empty()) {

			std::unordered_set<MapRequest*>::iterator iter =
				pending_map_requests.begin();

			while (!mapper_queue.empty() &&
			       iter != pending_map_requests.end()) {

				mapper = mapper_queue.front();
				mapper_queue.pop_front();

				mapper->sendMapRequest(*iter);
				iter++;
				
			}
		}

		AsyncMapCall *call;
		if (mapper_queue.empty()) {
			call = WorkerRpc::recvMapResponseSync();
			updateReduceRequests(call);
		}

		/* Wait 100ms for all remaining outstanding requests. If not
		 * all arrive, continue onward using available workers.
		 */
		std::chrono::system_clock::time_point deadline = tick(100);
		call = WorkerRpc::recvMapResponseAsync(deadline);
		while (call) {
			updateReduceRequests(call);
			call = WorkerRpc::recvMapResponseAsync(deadline);
		}
	}

	for (std::list<WorkerRpc*>::iterator iter = mapper_queue.begin();
	     iter != mapper_queue.end(); iter++) {
		reducer_queue.push_back(*iter);
	}
	for (int ii = 0; ii < reduce_requests.size(); ii++) {
		new_reduce_requests.push_back(&reduce_requests[ii]);
	}
	return true;
}


bool Master::manageReduceTasks() {

	WorkerRpc *reducer;
	while (!new_reduce_requests.empty() ||
	       !pending_reduce_requests.empty()) {

		while (!reducer_queue.empty() &&
		       !new_reduce_requests.empty()) {

			ReduceRequest *new_reduce_req;
			reducer = reducer_queue.front();
			reducer_queue.pop_front();

			new_reduce_req = new_reduce_requests.front();
			new_reduce_requests.pop_front();

			reducer->sendReduceRequest(new_reduce_req);
		}

		if (!reducer_queue.empty() &&
		    !pending_reduce_requests.empty()) {

			std::unordered_set<ReduceRequest*>::iterator iter =
				pending_reduce_requests.begin();

			while (!reducer_queue.empty() &&
			       iter != pending_reduce_requests.end()) {

				reducer = reducer_queue.front();
				reducer_queue.pop_front();

				reducer->sendReduceRequest(*iter);
				iter++;
			}
		}

		reap_old_map_requests(100);

		AsyncReduceCall *call;
		if (reducer_queue.empty()) {
			call = WorkerRpc::recvReduceResponseSync();
			processReducerOutput(call);
		}

		std::chrono::system_clock::time_point deadline = tick(100);
		call = WorkerRpc::recvReduceResponseAsync(deadline);
		while (call) {
			processReducerOutput(call);
			call = WorkerRpc::recvReduceResponseAsync(deadline);
		}
	}
	return true;
}

void Master::processReducerOutput(AsyncReduceCall *call) {
	return;
}

/* CS621_TASK: Here you go. once this function is called you will complete
 * whole map reduce task and return true if succeeded.
 */
bool Master::run() {
	return manageMapTasks() && manageReduceTasks();
}
