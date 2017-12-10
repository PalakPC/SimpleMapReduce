#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;
using grpc::ClientContext;
using grpc::Status;

using masterworker::MapperReducer;
using masterworker::MapRequest;
using masterworker::MapReply;
using masterworker::ReduceRequest;
using masterworker::ReduceReply;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;

class ReduceCallData {

private:
	static MapperReducer::AsyncService *reduce_service;
	static ServerCompletionQueue *red_cq;
	bool active;
	ServerContext ctx;
	ReduceRequest request;
	ReduceReply reply;
	friend class Worker;
	ServerAsyncResponseWriter<ReduceReply> responder;

public:
	static void initService(MapperReducer::AsyncService *serv,
				ServerCompletionQueue *queue);

	inline ReduceCallData() : responder(&ctx), active(true) {
		reduce_service->RequestReduceCall(&ctx, &request, &responder,
						  red_cq, red_cq, this);
	}

	inline void terminate() {
		delete this;
	}

	inline void replyToMaster() {
		active = false;
		responder.Finish(reply, Status::OK, this);
	}

	inline bool isActive() {
		return active;
	}
};
