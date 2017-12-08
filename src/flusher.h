#pragma once

#include <ofstream>

class Flusher {

public:
	Flusher(std::vector<ofstream> streams,
		std::vector<std::string> file_names);

	buffer(std::string key, std::string value);
	complete(&MapReply);

private:
	struct output {
		std::string ifile;
		ofstream istream;
	};
	std::unordered_map<std::string, output> ifile_map;
	struct output *buffer;
}
