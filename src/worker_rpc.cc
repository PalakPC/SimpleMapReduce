#include "worker_rpc.h"

WorkerRpc::WorkerRpc(CompletionQueue *cq,
		     std::shared_ptr<Channel> channel) :
	cq(cq),
	map_stub(Mapper::NewStub(channel)),
	reduce_stub(Reducer::NewStub(channel)) {}

void WorkerRpc::sendMapRequest(MapRequest *req) {
	
}


void WorkerRpc::sendReduceRequest(ReduceRequest *req) {
	
}

