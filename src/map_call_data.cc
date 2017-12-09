#include "map_call_data.h"

MapperReducer::AsyncService * MapCallData::map_service;
ServerCompletionQueue * MapCallData::map_cq;

void MapCallData::initService(MapperReducer::AsyncService *serv,
			      ServerCompletionQueue *queue) {
	map_service = serv;
	map_cq = queue;
}
