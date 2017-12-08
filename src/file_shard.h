#pragma once

#include "masterworker.grpc.pb.h"

using masterworker::MapperReducer;
using masterworker::MapRequest;
using masterworker::MapReply;
using masterworker::ShardInfo;

#include <list>
#include <vector>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here,
 * where you can hold information about file splits, that your
 * master would use for its own bookkeeping and to convey
 * the tasks to the workers for mapping
 */
struct FileShard {
	MapRequest shards;
};


/* CS6210_TASK: Create fileshards from the list of input files,
 * map_kilobytes etc. using mr_spec you populated.
 */
inline bool
shard_files(const MapReduceSpec& mr_spec,
	    std::vector<FileShard>& fileShards)
{
	const size_t gran = mr_spec.granularity << 10; /* KB --> bytes */
	size_t num_shards = (mr_spec.size + (gran - 1)) / gran;

	size_t begin = 0u;
	struct file_data cur = mr_spec.inputs.front();

	for(unsigned ii = 0u; ii < num_shards; ii++) {

		FileShard request;
		size_t to_read = gran;

		while (to_read) {
			
			ShardInfo *si = request.shards.add_shards();
			si->set_file_name(cur.file);
			si->set_begin(begin);

			if (to_read >= (cur.stats.st_size - begin)) {

				si->set_end(cur.stats.st_size);
				begin = 0u;
				to_read -= cur.stats.st_size - begin;
				cur = mr_spec.inputs.front();

			} else {
				
				si->set_end(begin + to_read);
				begin += to_read;
				to_read = 0u;
			}
		}
		fileShards.push_back(request);
	}
	return true;
}
