import multiprocessing as mp
import kafka #kafka-python package
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata

KAFKA_JUNCTION = 'localhost:9092'

# Number of partitions to replicate to
TARGET_PARTITION_COUNT = 32

SRC_GROUP_ID = 'antares_junction'
SRC_BOOTSTRAP_SERVERS = 'public.alerts.ztf.uw.edu:9092'

TRG_GROUP_ID = 'ztf'
TRG_BOOTSTRAP_SERVERS = 'localhost:9092'


def proc_replicate(topic, partition, end_offset):
    src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
                              bootstrap_servers=SRC_BOOTSTRAP_SERVERS,
                              auto_offset_reset='earliest')
    print(f"topic: {topic} part:{partition}")
    tp = TopicPartition(topic, partition)
    src.assign([tp])
    for msg in src:
        print(f"part={msg.partition} key={msg.key} offset={msg.offset}")


def replicate():
    # Get all source topics
    src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
                              bootstrap_servers=SRC_BOOTSTRAP_SERVERS)
    # example: ztf_20200102programid1
    src_topics_set = src.topics()
    src_topics = list(src_topics_set)
    src_topics.sort(key=lambda x: x[5:13], reverse=True)

    # Get all target topics
    trg = kafka.KafkaConsumer(group_id=TRG_GROUP_ID,
                              bootstrap_servers=TRG_BOOTSTRAP_SERVERS)
    trg_topics_set = trg.topics()
    trg_topics = list(trg_topics_set)
    trg_topics.sort(key=lambda x: x[5:13], reverse=True)

    # Determine if latest source topic is at least partially loaded to target
    the_topic = src_topics[4]

    part_set = src.partitions_for_topic(the_topic)
    src_partition_count = len(part_set)
    print(f"topic: {the_topic} # of partitions: {src_partition_count}")
    # Calculate multiplier for demuxing
    # Example:
    #    source = 4 target = 9 then multiplier is 9/4=2.25
    #    int(2.25) = 2
    multiplier = int(TARGET_PARTITION_COUNT / src_partition_count)
    trg_partition_count = src_partition_count * multiplier
    # Add the new topic in target cluster
    if the_topic not in trg_topics:
        print(f"replicate {the_topic} to {KAFKA_JUNCTION}")
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id='test'
        )
        topic_list = [NewTopic(name=the_topic, num_partitions=trg_partition_count, replication_factor=1)]
        print(f"Creating topic {the_topic} with {trg_partition_count} partitions")
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    # Get offset status for each partition
    print(f"Source broker partitions for topic {the_topic}")
    print("=========================================================================")
    parts = {}
    for p in src.partitions_for_topic(the_topic):
        tp = TopicPartition(the_topic, p)
        src.assign([tp])
        any_committed = src.committed(tp)
        committed = any_committed if any_committed else 0
        #consumer.seek_to_end(tp)
        src.seek_to_beginning(tp)
        last_offset = src.position(tp)
        end_offset = src.end_offsets([tp])
        parts[str(p)] = end_offset

        print("topic: %s partition: %s end offset: %s committed: %s last: %s lag: %s" %
              (the_topic, p, end_offset[tp], committed, last_offset, (last_offset - committed)))
        #consumer.seek_to_beginning(tp)

    src.close(autocommit=False)

    procs = [mp.Process(target=proc_replicate,
                        args=(the_topic, p, parts[str(p)])
                        ) for p in range(0, (src_partition_count-1))]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

if __name__ == '__main__':
    replicate()
