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

class MapCallData {

private:
	static MapperReducer::AsyncService *service;
	static ServerCompletionQueue *cq;
	bool active;
	ServerContext ctx;
	MapRequest request;
	MapReply reply;
	friend class Worker;
	ServerAsyncResponseWriter<MapReply> responder;

public:
	static void initService(MapperReducer::AsyncService *serv,
				ServerCompletionQueue *queue) {
		service = serv;
		cq = queue;
	}

        inline MapCallData() : responder(&ctx), active(true) {
		service->RequestMapCall(&ctx, &request, &responder,
					cq, cq, this);
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

class ReduceCallData {

private:
	static MapperReducer::AsyncService *service;
	static ServerCompletionQueue *cq;
	bool active;
	ServerContext ctx;
	ReduceRequest request;
	ReduceReply reply;
	ServerAsyncResponseWriter<ReduceReply> responder;

public:
	static void initService(MapperReducer::AsyncService *serv,
				ServerCompletionQueue *queue) {
		service = serv;
		cq = queue;
	}

	inline ReduceCallData() : responder(&ctx), active(true) {
		service->RequestReduceCall(&ctx, &request, &responder,
					cq, cq, this);
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
