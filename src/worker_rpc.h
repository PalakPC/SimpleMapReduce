#pragma once

#include "masterworker.grpc.pb.h"
#include "reduce_call_data.h"
#include "map_call_data.h"

/* Forward declaration */
class WorkerRpc;

struct AsyncMapCall {
	WorkerRpc *worker;
	MapRequest *request;
	ClientContext context;
	Status status;
	MapReply reply;
	std::unique_ptr<ClientAsyncResponseReader<MapReply>> reader;
};

struct AsyncReduceCall {
	WorkerRpc *worker;
	ReduceRequest *request;
	ClientContext context;
	Status status;
	ReduceReply reply;
	std::unique_ptr<ClientAsyncResponseReader<ReduceReply>> reader;
};

class WorkerRpc {

private:
	unsigned worker_id;
	static CompletionQueue mcq;
	static CompletionQueue rcq;
	std::unique_ptr<MapperReducer::Stub> stub;

public:
	/* Instance Functions for each worker */
	WorkerRpc(unsigned id, std::shared_ptr<Channel> channel);
	void sendMapRequest(MapRequest *req);
	void sendReduceRequest(ReduceRequest *req);

	/* Static Funtions for Master */
	static AsyncMapCall * recvMapResponseSync(void);
	static AsyncMapCall * recvMapResponseAsync(
		std::chrono::system_clock::time_point deadline);
	
	static AsyncReduceCall * recvReduceResponseSync(void);
	static AsyncReduceCall * recvReduceResponseAsync(
		std::chrono::system_clock::time_point deadline);
};

