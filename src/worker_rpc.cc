#include "worker_rpc.h"

CompletionQueue WorkerRpc::mcq;
CompletionQueue WorkerRpc::rcq;

WorkerRpc::WorkerRpc(unsigned id, std::shared_ptr<Channel> channel) :
        worker_id(id), stub(MapperReducer::NewStub(channel)) {}

void WorkerRpc::sendMapRequest(MapRequest *req) {

	req->set_worker_id(worker_id);
	AsyncMapCall *call = new AsyncMapCall;
	call->worker = this;
	call->request = req;
	call->reader = stub->PrepareAsyncMapCall(
		&call->context, *req, &mcq);

	call->reader->StartCall();
	call->reader->Finish(&call->reply, &call->status, (void*) call);
}

void WorkerRpc::sendReduceRequest(ReduceRequest *req) {

	req->set_worker_id(worker_id);
	AsyncReduceCall *call = new AsyncReduceCall;
	call->worker = this;
	call->request = req;
	call->reader = stub->PrepareAsyncReduceCall(
		&call->context, *req, &rcq);
	call->reader->StartCall();
	call->reader->Finish(&call->reply, &call->status, (void*) call);

}
	
AsyncMapCall * WorkerRpc::recvMapResponseSync(void) {

	void *tag = NULL;
	bool ok;
	AsyncMapCall *call;

	GPR_ASSERT(mcq.Next(&tag, &ok));
	GPR_ASSERT(ok);
	return static_cast<AsyncMapCall*>(tag);
}

AsyncMapCall * WorkerRpc::recvMapResponseAsync(
	std::chrono::system_clock::time_point deadline) {

	void *tag = NULL;
	bool ok = false;
	AsyncMapCall *call;

	/* Not quite sure what the function of ok is ??*/
	GPR_ASSERT(mcq.AsyncNext(&tag, &ok, deadline));
	return (ok) ? static_cast<AsyncMapCall*>(tag) : NULL;
}


AsyncReduceCall * WorkerRpc::recvReduceResponseSync(void) {

	void *tag;
	bool ok;
	AsyncReduceCall *call;

	GPR_ASSERT(rcq.Next(&tag, &ok));
	GPR_ASSERT(ok);
	return static_cast<AsyncReduceCall*>(tag);
}

AsyncReduceCall * WorkerRpc::recvReduceResponseAsync(
	std::chrono::system_clock::time_point deadline) {

	void *tag;
	bool ok = false;
	AsyncReduceCall *call;

	GPR_ASSERT(rcq.AsyncNext(&tag, &ok, deadline));
	return (ok) ? static_cast<AsyncReduceCall*>(tag) : NULL;
}
