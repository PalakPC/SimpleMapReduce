#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"

#include <iostream>
#include <fstream>
#include <cstdio>

using masterworker::MapReply;

class Flusher {

public:
	Flusher(MapReply *map_reply, unsigned buffer_size);
	~Flusher();
	void buffer(std::string key, std::string value);
	void flush_key_values(void);

private:
	unsigned buf_index;
	unsigned num_reducers;
	size_t capacity;
	struct pair {
		std::string key;
		std::string value;
	};
	struct pair *key_value_buffer;
	std::vector<std::ofstream> output_streams;
};
