#include "flusher.h"

#include <functional>


Flusher::Flusher(MapReply *map_reply, unsigned buffer_size) {

	buf_index = 0;
	capacity = buffer_size / sizeof(*key_value_buffer);
	key_value_buffer = new pair[capacity];

	for (int ii = 0; ii < map_reply->ifiles_size(); ii++) {
		std::string ifile = map_reply->ifiles(ii);
		std::ofstream stream(ifile);
		if (!stream.is_open()) {
			std::cerr << "Failed to open file: " <<
				ifile << std::endl;
			return;
		}
		output_streams.push_back(stream);
	}
	num_reducers = output_streams.size();
}

Flusher::~Flusher() {

	std::vector<std::ofstream>::iterator iter;
	for (iter = output_streams.begin(); iter != output_streams.end();
	     iter++) {
		(*iter).close();
	}
	delete key_value_buffer;
}

void Flusher::buffer(std::string key, std::string value) {

	if (buf_index == capacity) {
		flush_key_values();
	}
	key_value_buffer[buf_index].key = key;
	key_value_buffer[buf_index].value = value;
	buf_index++;
}

void Flusher::flush_key_values(void) {

	std::hash<std::string> hash_fn;
	for (unsigned ii = 0; ii < capacity; ii++) {
		struct pair *cur = &key_value_buffer[ii];
		output_streams[hash_fn(cur->key) % num_reducers]
			<< cur->key << ", " << cur->value << std::endl;
	}
	buf_index = 0;
}
