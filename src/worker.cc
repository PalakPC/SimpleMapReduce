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



bool Worker::processMapRequest(void) {

	if (!mcall->isActive()) {
		mcall->terminate();
		mcall = new MapCallData();
		return;
	}

	/* Create unique output file steams */
	unsinged num_ifiles = mcall->request.num_reducers();
	
	std::vector<std::string> ifile_names;
	std::vector<ofstream> streams(num_ifiles);
	
	for (unsigned ii = 0; ii < num_ifiles; ii++) {
		
		ofstream ifile = streams.at(ii);
		std::string ifile_name = getUniqueFile(ii);		
		ifile.open(ifile_name);

		if (!ifile.is_open()) {
			goto clean_up;
		}
	}
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->impl_->set_outputs(streams);
	
	/* TODO: Open and process shards. */
	
	num_mappings_processed++;
	
clean_up:
	for (unsinged ii = 0; ii < num_ifiles; ii++) {
		ofstream ifile = streams.at(ii);
		if (!ifile.is_open)
			return;
		ifile.close();
	}
}

std::string Worker::genUniqueFile(unsinged tag) {
	
	std::string out_file = addr_port +
		std::to_string(num_mappings_processed) +
		std::to_string(tag);

	
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
