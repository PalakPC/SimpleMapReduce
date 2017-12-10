#include "flusher.h"
#include <functional>


Flusher::Flusher(MapReply *map_reply, unsigned buffer_size) {

	buf_index = 0;
	capacity = buffer_size / sizeof(*key_value_buffer);
	key_value_buffer = new pair[capacity];

	for (int ii = 0; ii < map_reply->ifiles_size(); ii++) {
		std::string ifile = map_reply->ifiles(ii);
		output_streams.emplace_back(std::ofstream(ifile.c_str()));
		if (!output_streams[ii].is_open()) {
			std::cerr << "Failed to open file: " <<
				ifile << std::endl;
			return;
		}
	}
	num_reducers = output_streams.size();
}

Flusher::~Flusher() {

	for (int ii = 0; ii < output_streams.size(); ii++) {
		output_streams[ii].close();
	}
	delete[] key_value_buffer;
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
	unsigned stop = (buf_index == capacity) ? capacity : buf_index + 1;
	for (unsigned ii = 0; ii < stop; ii++) {
		struct pair *cur = &key_value_buffer[ii];
		output_streams[hash_fn(cur->key) % num_reducers]
			<< cur->key << "," << cur->value << std::endl;
	}
	buf_index = 0;
}
