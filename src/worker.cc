#include "worker.h"


/* CS6210_TASK: ip_addr_port is the only information you get when started.
 * You can populate your other class data members here if you want
 */
Worker::Worker(std::string ip_addr_port) {

}

/* CS6210_TASK: Here you go. once this function is called
 * your woker's job is to keep looking for new tasks
 * from Master, complete when given one and again keep looking
 * for the next one. Note that you have the access to BaseMapper's
 * member BaseMapperInternal impl_ and BaseReduer's membe
 * BaseReducerInternal impl_ directly, so you can manipulate them
 * however you want when running map/reduce tasks.
 */
bool Worker::run() {
	
	/*  Below 5 lines are just examples of how you will call map and reduce
	 *  Remove them once you start writing your own logic
	 */
	
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	
	mapper->map("I m just a 'dummy', a \"dummy line\"");
	auto reducer = get_reducer_from_task_factory("cs6210");
	
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	return true;
}
