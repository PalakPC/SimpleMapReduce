#include "worker.h"


/* CS6210_TASK: ip_addr_port is the only information you get when started.
 * You can populate your other class data members here if you want
 */
Worker::Worker(std::string ip_addr_port) {

	builder.AddListeningPort(ip_addr_port,
				 grpc::InsecureServerCredentials());

	builder.RegisterService(&service);
	queue = builder.AddCompletionQueue();
	
	MapCallData::initService(&service, queue.get());
	ReduceCallData::initService(&service, queue.get());

	mcall = new MapCallData();
	rcall = new ReduceCallData();
}

bool Worker::recvMapRequest(void) {

	void *tag;
	bool ok;
	queue->Next(&tag, &ok);
	GPR_ASSERT(ok);
	return (tag == (void*) mcall);
}



void Worker::processMapRequest(void) {

	if (mcall->isActive()) {

		MapRequest map_request = mcall->request;
		MapReply map_reply = mcall->reply;
		unsigned num_reducers = map_request.num_reducers();
		for (int red_id = 0;  red_id < num_reducers; red_id++) {
			map_reply.add_ifiles(genUniqueFile(&map_request, red_id));
		}

		auto mapper = get_mapper_from_task_factory(map_request.user_id());
		Flusher map_flusher(&map_reply, 2048); /*Wiil fix!!!!!!!!!*/
		mapper->impl_->map_flusher = &map_flusher;

		/* Add logic for file seeking and calling map here */

		mapper->impl_->map_flusher->flush_key_values();

	} else {
		mcall->terminate();
		mcall = new MapCallData();
	}
	
}

std::string Worker::genUniqueFile(MapRequest *req, int reducer_id) {

	unsigned map_id = req->mapper_id();
	unsigned work_id = req->worker_id();
	std::string ifile = std::to_string(map_id) + "_" +
		std::to_string(reducer_id) + "_" + std::to_string(work_id);
	
}

/* CS6210_TASK: Here you go. once this function is called
 * your woker's job is to keep looking for new tasks
 * from Master, complete when given one and again keep looking
 * for the next one. Note that you have the access to BaseMapper's
 * member BaseMapperInternal impl_ and BaseReduer's member
 * BaseReducerInternal impl_ directly, so you can manipulate them
 * however you want when running map/reduce tasks.
 */
bool Worker::run() {
	
	/*  Below 5 lines are just examples of how you will call map and reduce
	 *  Remove them once you start writing your own logic
	 */
	
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;


	while (recvMapRequest()) {
		processMapRequest();
	}
	return true;
}
