The World's Greatest Map Reduce System Ever. Even Better than Google's.

By: Palak Choudhary && Eric Martin

Framework and Master

Master maintains two queues for handling workers during the map phase and
the reduce phase. If either queue is empty during either phase, then the master
knows that all nodes are busy and will not attempt to send out new/old
map/reduce requests.

All NEW map and reduce requests are submitted to available workers in the
queues FIRST. If the the queues are empty, then master blocks waiting for
at least one worker to return their results. Otherwise, a deadline a set
for the other workers to arrive. If not all workers return results within
a deadline, the mapper will continue sending new and old requests to availble
workers.

Old map/reduce requests from stragling nodes are saved in unordered_sets.
If all the new requests are submitted, then master simply pulls an old request
form these sets and submits it to an availble worker.

At the end of the map phase, if there are old outstanding map requests, then
the master will listen for them only after it has sent all of its fresh reduce
requests.

Map/Reduce

During the map phase, we added a "Flusher class" which buffers key_value pairs
up to 1/8 of the input shard size. When the buffer is full, it simply flushes to
disk.

Unique file names are generated for intermediate mapper files and the reducers files.

Intermediate Format: mapper_workerID_mapperID_bucketID
Output FormatL reducer_workerID_reducerID

Note the bucketID == reducerID. The keys encountered are hashed and compressed to
the defined number of reducers.

And of course, the worker nodes comminicate asynchronously with the master node. :)

When Running:

to build source code ---> hit 'make' in src directory
Our program will replace intermedaite files that it needs in order save key, value pairs
from the map phase. So it should work on successive runs. Change the config as you like.
And Have a Merry Christmas! (Or Happy Holiday. Which ever you prefer :) ).



/**************************************************************************************/
/******************************IMPORTANT***********************************************/
/**************************************************************************************/

Now... C++
We decided to add actual .cc source files. Because it is important!!!
However, when you build a library in C++ and have static class members you must
declare them outside the scope of the class. This error does not occur during
compile time. It occurs during link time when you build the archive. Why!! GOD KNOWS!!!

How anyone builds an interface using C++ is beyond me. That said objects cool, but still
why. Is it a feature?



