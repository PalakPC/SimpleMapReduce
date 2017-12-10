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
using masterworker::ShardInfo;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;

class MapCallData {

private:
	static MapperReducer::AsyncService *map_service;
	static ServerCompletionQueue *map_cq;
	bool active;
	ServerContext ctx;
	MapRequest request;
	MapReply reply;
	friend class Worker;
	ServerAsyncResponseWriter<MapReply> responder;

public:
	static void initService(MapperReducer::AsyncService *serv,
				ServerCompletionQueue *queue);

        inline MapCallData() : responder(&ctx), active(true) {
		map_service->RequestMapCall(&ctx, &request, &responder,
					    map_cq, map_cq, this);
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

MapperReducer::AsyncService * MapCallData::map_service;
ServerCompletionQueue * MapCallData::map_cq;
void MapCallData::initService(MapperReducer::AsyncService *serv,
			 ServerCompletionQueue *queue) {
	map_service = serv;
	map_cq = queue;
}

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

MapperReducer::AsyncService * ReduceCallData::reduce_service;
ServerCompletionQueue * ReduceCallData::red_cq;
void ReduceCallData::initService(MapperReducer::AsyncService *serv,
				 ServerCompletionQueue *queue) {
	reduce_service = serv;
	red_cq = queue;
}
