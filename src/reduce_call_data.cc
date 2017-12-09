#include "reduce_call_data.h"

MapperReducer::AsyncService * ReduceCallData::reduce_service;
ServerCompletionQueue * ReduceCallData::red_cq;

void ReduceCallData::initService(MapperReducer::AsyncService *serv,
			      ServerCompletionQueue *queue) {
	reduce_service = serv;
	red_cq = queue;
}
