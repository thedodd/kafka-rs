# Producer partitioning strategies
Producers are applications that write data to partitions in Kafka topics. Kafka provides the following partitioning strategies when producing a message.

## Default Partitioner
The default strategy for producing messages. When the key is null, the record is sent randomly to one of the available partitions of the topic. If a key exists, the partitioner hashes the key, and the result is used to map the message to a specific partition. This ensures that messages with the same key end up in the same partition. This mapping, however, is consistent only as long as the number of partitions in the topic remains the same. If new partitions are added, new messages with the same key might get written to a different partition than old messages with the same key.

## Round Robin Partitioner
Use this approach when the producer wants to distribute the writes equally among all partitions. This distribution is irrespective of the key’s hash value (or the key being null), so messages with the same key can end up in different partitions.

This strategy is useful when the workload becomes skewed by a single key, meaning that many messages are being produced for the same key. Suppose the ordering of messages is immaterial and the default partitioner is used. In that case, imbalanced load results in messages getting queued in partitions and an increased load on a subset of consumers to which those partitions are assigned. The round-robin strategy will result in an even distribution of messages across partitions.

## Uniform Sticky Partitioner
Currently, when no partition and key are specified, a producer’s default partitioner partitions records in a round-robin fashion. That means that each record in a series of consecutive records will be sent to a different partition until all the partitions are covered, and then the producer starts over again. While this spreads records out evenly among the partitions, it also results in more batches that are smaller in size, leading to more requests and queuing as well as higher latency.

The uniform sticky partitioner was introduced to solve this problem. It has two rules:
1. If a partition is specified with the record, that partition is used as it is.
2. If no partition is specified, a sticky partition is chosen until the batch is full or `linger.ms` (the time to wait before sending messages) is up.

“Sticking” to a partition enables larger batches and reduces latency in the system. After sending a batch, the sticky partition changes. Over time, the records are spread out evenly among all the partitions.

The record key is not used as part of the partitioning strategy, so records with the same key are not guaranteed to be sent to the same partition.
