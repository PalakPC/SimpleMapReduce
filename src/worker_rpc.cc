#include "worker_rpc.h"

WorkerRpc::WorkerRpc(CompletionQueue *cq,
		     std::shared_ptr<Channel> channel) :
	cq(cq),
	map_stub(Mapper::NewStub(channel)),
	reduce_stub(Reducer::NewStub(channel)) {}

void WorkerRpc::sendMapRequest(MapRequest *req) {

	AsyncMapCall *call = new AsyncMapCall;
	call->worker = this;
	call->request = req;
	call->reader = map_stub->PrepareAsyncMapCall(
		&call->context, *req, cq);

	call->reader->StartCall();
	call->reader->Finish(&call->reply, &call->status, (void*) call);
}


void WorkerRpc::sendReduceRequest(ReduceRequest *req) {

	AsyncReduceCall *call = new AsyncReduceCall;
	call->worker = this;
	call->request = req;
	call->reader = reduce_stub->PrepareAsyncReduceCall(
		&call->context, *req, cq);
	call->reader->StartCall();
	call->reader->Finish(&call->reply, &call->status, (void*) call);

}

