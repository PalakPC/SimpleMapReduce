#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::Status;

using masterworker::Mapper;
using masterworker::Reducer;
using masterworker::MapRequest;
using masterworker::MapReply;
using masterworker::ReduceRequest;
using masterworker::ReduceReply;

class WorkerRpc {

private:
	CompletionQueue *cq;
	std::unique_ptr<Mapper::Stub> map_stub;
	std::unique_ptr<Reducer::Stub> reduce_stub;

public:
	WorkerRpc(CompletionQueue *cq,
		  std::shared_ptr<Channel> channel);
	void sendMapRequest(MapRequest *req);
	void sendReduceRequest(ReduceRequest *req);
};

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
