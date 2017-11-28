#pragma once

#include <string>
#include <vector>
#include <list>
#include <fstream>
#include <iostream>
#include <sstream>
#include <istream>
#include <map>
#include <sys/stat.h>

#define LINE 512

/* CS6210_TASK: Create your data structure here for storing spec
 * from the config file
 */
struct file_data {
	struct stat stats;
	const char *file;
};

struct MapReduceSpec {
	size_t granularity;
	size_t size;
	std::vector<std::string> addrs;
	std::vector<struct file_data> inputs;
};

enum mr_spec_type {
	CONFIG_IGNORE = -1,
	SHARD_GRANULARITY,
	IP_ADDRS,
	INPUT_FILES
};

inline mr_spec_type
resolve_mr_type(std::string type)
{
	static const std::map<std::string, mr_spec_type> mapper {
		{"worker_ipaddr_ports", IP_ADDRS},
		{"input_files", INPUT_FILES},
		{"map_kilobytes", SHARD_GRANULARITY}
	};

	auto iter = mapper.find(type);
	if (iter != mapper.end()) {
		return iter->second;
	}
	return CONFIG_IGNORE;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the
 * specification from the config file.
 */
inline bool
read_mr_spec_from_config_file(const std::string& config_filename,
			      MapReduceSpec& mr_spec)
{
	mr_spec.size = 0u;

	std::ifstream stream(config_filename);
	if (!stream.is_open()) {
		std::cerr << "Failed to open an IO stream for " <<
			config_filename << std::endl;
		return (stream.close(), false);
	}

	std::string line;
	while (std::getline(stream, line)) {
		int del = line.find("=");
		if (del < 0) {
			std::cerr << "Input Config file lacks a '=' token."
				  << std::endl;
			return (stream.close(), false);
		}

		std::string type = line.substr(0, del);
		std::string value = line.substr(del + 1);

		std::string token;
		switch(resolve_mr_type(type)) {

		case SHARD_GRANULARITY:
			mr_spec.granularity = stoi(value);
			break;
		case IP_ADDRS:
		{
			std::stringstream ss(value);
			while(getline(ss, token, ',')) {
				mr_spec.addrs.push_back(token);
			}
			break;
		}
		case INPUT_FILES:
		{
			std::stringstream ss(value);
			while(getline(ss, token, ',')) {
				struct file_data cur;
				cur.file = token.c_str();
				if (stat(cur.file, &cur.stats) < 0) {
					std::cerr << "Failed to obtain meta "
						"data for file " << token <<
						std::endl;
				} else {
					mr_spec.size += cur.stats.st_size;
					mr_spec.inputs.push_back(cur);
				}
			}
			break;
		}
		case CONFIG_IGNORE:
			break;
		}
	}

	return (stream.close(), true);
}


/* CS6210_TASK: validate the specification read from the config file.
 */
inline bool
validate_mr_spec(const MapReduceSpec& mr_spec)
{
	return mr_spec.inputs.size() > 0 && mr_spec.granularity > 0;
}
