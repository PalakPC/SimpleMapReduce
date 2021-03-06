#include "worker.h"
# include <cstdio>

/* CS6210_TASK: ip_addr_port is the only information you get when started.
 * You can populate your other class data members here if you want
 */
Worker::Worker(std::string ip_addr_port) {

	worker_addr = ip_addr_port;
	builder.AddListeningPort(worker_addr,
				 grpc::InsecureServerCredentials());

	builder.RegisterService(&service);
	queue = builder.AddCompletionQueue();

	MapCallData::initService(&service, queue.get());
	ReduceCallData::initService(&service, queue.get());

	server = builder.BuildAndStart();

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

bool Worker::recvReduceRequest(void) {

	void *tag;
	bool ok;
	queue->Next(&tag, &ok);
	GPR_ASSERT(ok);
	return (tag == (void*) rcall);
}

void Worker::processMapRequest(void) {

	if (mcall->isActive()) {

		MapRequest map_request = mcall->request;
		unsigned num_reducers = map_request.num_reducers();
		for (int red_id = 0;  red_id < num_reducers; red_id++) {
			std::string *ifile = mcall->reply.add_ifiles();
			ifile->assign(genUniqueFile(&map_request, red_id));
		}
		
		auto mapper = get_mapper_from_task_factory(map_request.user_id());
		Flusher map_flusher(&mcall->reply, map_request.buffer_size());
		mapper->impl_->map_flusher = &map_flusher;

		std::vector<ShardInfo> mapShards;
		std::ifstream file;
		for (int i = 0; i < map_request.shard_size(); i++) {
			mapShards.push_back(map_request.shard(i));
		}

		for (int i = 0; i < mapShards.size(); i++) {
			file.open(mapShards[i].file_name());
			char c;
			std::string sentence;

			if (mapShards[i].begin() == 0) {
				file.seekg(mapShards[i].begin());
			} else {
				file.seekg(mapShards[i].begin() - 1);
				file.get(c);
				if (c != '\n') {
					std::getline(file, sentence);
				}
			}

			while (file.tellg() < mapShards[i].end()) {
				std::getline(file, sentence);
				mapper->map(sentence);
			}
		}
		file.close();
		mapper->impl_->map_flusher->flush_key_values();
		mcall->replyToMaster();
		
	} else {
		mcall->terminate();
		mcall = new MapCallData();
	}
}

void Worker::processReduceRequest(void) {

	if (rcall->isActive()) {
		ReduceRequest reduce_request = rcall->request;

		std::vector<std::string> fileNames;
		std::map<std::string, std::vector<std::string>> collection;
		
		for (int i = 0; i < reduce_request.files_size(); i++) {
			fileNames.push_back(reduce_request.files(i));
		}
		
		auto reducer = get_reducer_from_task_factory(reduce_request.user_id());
		std::string out_file_name("reducer_" +
			std::to_string(reduce_request.worker_id())
			+ "_" + std::to_string(reduce_request.reducer_id()));

		remove(out_file_name.c_str());
		reducer->impl_->out_file_name = out_file_name;

		for (int i = 0; i < fileNames.size(); i++) {
			std::ifstream file;
			file.open(fileNames[i]);
			std::string sentence;
			while (getline(file, sentence)) {
				std::string key;
				std::string value;
				size_t pos = sentence.find(",");
				if (pos != std::string::npos) {

					key = sentence.substr(0, pos);
					value = sentence.substr(pos + 1, sentence.size());

					if (collection.find(key) != collection.end()) {
						collection[key].push_back(value);
					} else {
						std::vector<std::string> firstValue;
						firstValue.push_back(value);
						collection[key] = firstValue;
					}
				}
			}
			file.close();
		}

		for (auto i = collection.begin(); i != collection.end(); i++) {
			reducer->reduce(i->first, i->second);
		}

      rcall->reply.set_output_file(out_file_name);
		rcall->reply.set_iscomplete(true);
		rcall->replyToMaster();

	} else {
		rcall->terminate();
		rcall = new ReduceCallData();
	}
}

std::string Worker::genUniqueFile(MapRequest *req, int reducer_id) {
	return "mapper_ifile_" + std::to_string(req->worker_id()) + "_" +
		std::to_string(req->mapper_id()) + "_" +
		std::to_string(reducer_id);
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

	while (true) {
		while (recvMapRequest()) {
			processMapRequest();
		}
		processReduceRequest();
	}
	return true;
}
