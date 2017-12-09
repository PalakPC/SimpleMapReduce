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
	MapRequest mapRequest;
};


/* CS6210_TASK: Create fileshards from the list of input files,
 * map_kilobytes etc. using mr_spec you populated.
 */
inline bool
shard_files(const MapReduceSpec& mr_spec,
	    std::vector<FileShard>& fileShards)
{
	const size_t gran = mr_spec.granularity;
	size_t num_shards = (mr_spec.size + (gran - 1)) / gran; /* Round up */

	size_t file_offset = 0u;
	std::vector<struct file_data>::const_iterator input_file;
	input_file = mr_spec.inputs.begin();

	for(unsigned map_id = 0u; map_id < num_shards; map_id++) {

		FileShard fileShard;
		MapRequest *map_req = &fileShard.mapRequest;
		map_req->set_user_id(mr_spec.user_id);
		map_req->set_num_reducers(num_shards);
		map_req->set_mapper_id(map_id);
		map_req->set_buffer_size(gran >> 3);

		size_t to_read = gran;
		while (to_read) {

			struct file_data cur = *input_file;
			ShardInfo *shardInfo = map_req->add_shard();
			shardInfo->set_file_name(cur.file);
			shardInfo->set_begin(file_offset);

			if (to_read >= (cur.stats.st_size - file_offset)) {

				shardInfo->set_end(cur.stats.st_size);
				to_read -= cur.stats.st_size - file_offset;
				file_offset = 0u;
				++input_file;

			} else {

				file_offset += to_read;
				shardInfo->set_end(file_offset);
				to_read = 0u;
			}
		}
		fileShards.push_back(fileShard);
	}
	return true;
}
