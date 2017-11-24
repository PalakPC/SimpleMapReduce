#pragma once

#include <list>
#include <cmath>
#include <vector>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here,
 * where you can hold information about file splits, that your
 * master would use for its own bookkeeping and to convey
 * the tasks to the workers for mapping
 */
struct file_offsets {
	size_t begin, end;
	std::string file;
};

struct FileShard {
	bool isComplete;
	std::vector<struct file_offsets> files;
};


/* CS6210_TASK: Create fileshards from the list of input files,
 * map_kilobytes etc. using mr_spec you populated.
 */
inline bool
shard_files(const MapReduceSpec& mr_spec,
	    std::vector<FileShard>& fileShards)
{
	const size_t gran = mr_spec.granularity;
	size_t num_shards = (mr_spec.size + (gran - 1)) / gran;

	size_t begin = 0u;
	struct file_data cur = mr_spec.inputs.front();

	for(unsigned ii = 0u; ii < num_shards; ii++) {

		FileShard shard;
		shard.isComplete = false;
		size_t to_read = gran;

		while (to_read) {
			
			struct file_offsets fo;
			fo.file = cur.file;
			fo.begin = begin;

			if (to_read >= (cur.stats.st_size - begin)) {

				fo.end = cur.stats.st_size;
				shard.files.push_back(fo);
				begin = 0u;
				to_read -= cur.stats.st_size - begin;
				cur = mr_spec.inputs.front();

			} else {
				
				fo.end = begin + to_read;
				shard.files.push_back(fo);
				begin += to_read;
				to_read = 0u;
			}
		}
		fileShards.push_back(shard);
	}
	return true;
}
