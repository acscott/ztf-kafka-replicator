import sys
import argparse
import time
import multiprocessing as mp
import kafka #kafka-python package
import lz4  # Not necesssary to import lz4, but it does need to be installed for the kafka module
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata

# Number of partitions to replicate to
TARGET_PARTITION_COUNT = 32

SRC_GROUP_ID = 'antares_junction'
SRC_BOOTSTRAP_SERVERS = 'public.alerts.ztf.uw.edu:9092'

TRG_GROUP_ID = 'ztf'
TRG_BOOTSTRAP_SERVERS = 'localhost:9092'

# Number of messages each thread produces between outputting stats in the log
LOG_INTERVAL = 250


def outputstat(t0, t1, interval, partition, offset, msg_count, ending_offset):

    pcntcomplete = "{:.2%}".format(msg_count / ending_offset)
    t2 = time.time()
    mps = msg_count / (t2 - t0)
    t1 = t2
    mpsf = "{:.2f}".format(mps)
    leftsecs = (ending_offset - msg_count) / mps
    m, s = divmod(int(leftsecs), 60)
    h, m = divmod(m, 60)

    print(f"\033[F{partition:03}:{offset} - {mpsf} mps, {pcntcomplete} eta:" +
          '{:d}h:{:02d}m:{:02d}s'.format(h, m, s))
    return t1


def proc_replicate(topic, src_partition, end_offset, part_map, reset=False):
    """
      part_map list[list[]]
    """

    src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
                              bootstrap_servers=SRC_BOOTSTRAP_SERVERS,
                              auto_offset_reset='earliest',
                              consumer_timeout_ms=300000)
    print(f"topic: {topic} part:{src_partition}")
    tp = TopicPartition(topic, src_partition)
    src.assign([tp])
    if reset:
        print(f"Resetting source partition {src_partition} to beginning")
        src.seek_to_beginning(tp)

    trg = kafka.KafkaProducer(client_id=TRG_GROUP_ID,
                              bootstrap_servers=TRG_BOOTSTRAP_SERVERS)
    trg_part_ndx = 0
    trg_part_ndx_max = len(part_map[src_partition])-1  # ex: a length of 2 has 1 as the max

    msg_count = 0

    t0 = t1 = time.time()
    ending_offset = end_offset[list(end_offset)[0]]

    try:
        for msg in src:
            msg_count += 1
            trg.send(topic, value=msg.value, partition=part_map[src_partition][trg_part_ndx])
            # 300 secs, we must do this if we want to ensure not to lose messages
            # learned of during testing; w/o it, messages do get produced but many, many
            # will not show up in the target cluster
            trg.flush(300)
            trg_part_ndx += 1
            if trg_part_ndx > trg_part_ndx_max:
                trg_part_ndx = 0

            # Print status/stats
            if msg_count % LOG_INTERVAL == 0:
                t1 = outputstat(t0, t1, LOG_INTERVAL, src_partition, msg.offset, msg_count, ending_offset)
    except StopIteration:
        print(f"source partition {src_partition} no messages in 300 seconds, ending")

    print(f"source partition {src_partition:03} complete ========================")
    t1 = outputstat(t0, t1, LOG_INTERVAL, src_partition, msg.offset, msg_count, ending_offset)


def create_part_map(src_partition_count, multiplier):
    # create target partition map: 16 parts with 32 output parts
    # map[0] = [0,1]
    # map[1] = [2,3]
    # map[2] = [4,5]
    # ...
    # map[16] = [31,32]
    print("Creating source and target partition map")
    part_map = []
    trg_partition_ndx = 0
    for i in range(0, src_partition_count):
        print(f"[{i}] = [", end="")
        parts = []
        for n in range(multiplier):
            parts.append(trg_partition_ndx)
            if n == multiplier-1:
                print(f"{trg_partition_ndx}] ", end="")
            else:
                print(f"{trg_partition_ndx},", end="")
            trg_partition_ndx += 1
        part_map.append(parts)

    print(".")
    return part_map


def replicate(topic, reset, source, src_groupid, target, trg_groupid, trg_partitions):

    # Connect to source kafka cluster
    src = kafka.KafkaConsumer(group_id=src_groupid,
                              bootstrap_servers=source)
    # Connect ot target kafka cluster
    trg = kafka.KafkaConsumer(group_id=trg_groupid,
                              bootstrap_servers=target)

    # Determine if latest source topic is at least partially loaded to target
    trg_topics, the_topic = determine_topic(topic, src, trg, reset)

    part_set = src.partitions_for_topic(the_topic)
    src_partition_count = len(part_set)
    print(f"topic: {the_topic} # of partitions: {src_partition_count}")
    # Calculate multiplier for demuxing
    # Example:
    #    source = 4 target = 9 then multiplier is 9/4=2.25
    #    int(2.25) = 2
    multiplier = int(trg_partitions / src_partition_count)
    trg_partition_count = src_partition_count * multiplier
    print(f"multiplier={multiplier} target_partition_count={trg_partition_count}")

    # Add the new topic in target cluster
    if the_topic not in trg_topics:
        print(f"replicate {the_topic} to {TRG_BOOTSTRAP_SERVERS} with group id: {src_groupid}")
        admin_client = KafkaAdminClient(
            bootstrap_servers=TRG_BOOTSTRAP_SERVERS,
            client_id=TRG_GROUP_ID
        )
        topic_list = [NewTopic(name=the_topic, num_partitions=trg_partition_count, replication_factor=1)]
        print(f"Creating topic {the_topic} with {trg_partition_count} partitions")
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    part_map = create_part_map(src_partition_count, multiplier)

    # Get offset status for each partition
    print(f"Source broker partitions for topic {the_topic}")
    print("-------------------------------------------------------------------------")
    parts = {}
    total_committed = 0
    total_offsets = 0
    for p in src.partitions_for_topic(the_topic):
        tp = TopicPartition(the_topic, p)
        src.assign([tp])
        any_committed = src.committed(tp)
        committed = any_committed if any_committed else 0
        total_committed += committed
        #consumer.seek_to_end(tp)
        last_offset = src.position(tp)
        end_offset = src.end_offsets([tp])
        parts[str(p)] = end_offset
        total_offsets += end_offset[tp]
        print("Source topic: %s partition: %s end offset: %s committed: %s last: %s lag: %s" %
              (the_topic, p, end_offset[tp], committed, last_offset, (last_offset - committed)))
        #consumer.seek_to_beginning(tp)

    src.close(autocommit=False)
    print(f"Source: total_committed={total_committed} total_offsets={total_offsets}")
    print("=========================================================================")

    print(f"Starting multi-process: the_topic={the_topic} reset={reset} src_partition_count={src_partition_count}")
    procs = [mp.Process(target=proc_replicate,
                        args=(the_topic, p, parts[str(p)], part_map, reset)
                        ) for p in range(0, src_partition_count)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


def get_topic_offset_sum(topic, cluster):
    topic_offsets_sum = 0
    for part in cluster.partitions_for_topic(topic):
        tp = TopicPartition(topic, part)
        cluster.assign([tp])
        end_offset = cluster.end_offsets([tp])
        topic_offsets_sum += end_offset[list(end_offset)[0]]
    return topic_offsets_sum


def determine_topic(topic, src, trg, reset):

    if topic:
        if not reset:
            # is the topic in the target and is the sum of end offsets smaller than the source?
            trg_offset_sum = get_topic_offset_sum(topic, trg)
            src_offset_sum = get_topic_offset_sum(topic, src)
            if trg_offset_sum < src_offset_sum:
                the_topic=topic
            else:
                print("Target sum of ending offsets of each partition is not less than the Source")
                print(f"trg_offset_sum={trg_offset_sum} src_offset_sum={src_offset_sum}")
                print(f"Use the reset option to reload the topic {topic}")
                sys.exit(0)
        else:
            the_topic = topic
    else:
        # automatically determine the topic to load
        src_topics_set = src.topics()
        src_topics = list(src_topics_set)
        src_topics.sort(key=lambda x: x[5:13], reverse=True)
        print_source_topics(src_topics)

        trg_topics_set = trg.topics()
        trg_topics = list(trg_topics_set)
        trg_topics.sort(key=lambda x: x[5:13], reverse=True)

        # Find the latest src topic not in trg
        missing_topics = list_items_not_in(src_topics, trg_topics)
        if len(missing_topics) == 0:
            print("No topics are missing in the target cluster, will look for incomplete topics ")
            candidate_topics = src_topics
        else:
            candidate_topics = missing_topics

        candidate_topics.sort(key=lambda x: x[5:13], reverse=True)
        the_topic = None
        for candidate_topic in candidate_topics:
            offset_sum = get_topic_offset_sum(candidate_topic, src)
            if offset_sum != 0:
                the_topic = candidate_topic
                break

        if not the_topic:
            print("All candidate source topics have 0 offset sums. Nothing to replicate. ")
            sys.exit(0)
        else:
            print(f"Automatic determination of topic: {the_topic} with {offset_sum} sum of ending offsets of all "
                  "partitions")

    return trg_topics, the_topic


def list_items_not_in(list1, list2):
    return list(set(list1) - set(list2))


def print_source_topics(src_topics):
    print("Source topics")
    print("--------------------------------------")
    i = 0
    for item in src_topics:
        print(f"{i:003}:{item}")
        i = i + 1
    print("======================================")


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='kafka-replicator',
                                description="Replicate and demux a topic's partitions from a source to target "
                                "Kafka cluster")
    p.add_argument('-t', '--topic', action='store_true',
                   help="The name of the source topic.  Will use the latest one indicated"
                        "by the date for ztf and not present in target if not specified.")
    p.add_argument('-r', '--reset', action='store_true', default=False,
                   help="Forces the Consumer to start from the beginning.  There's a chance you will duplicate "
                        "messages to the Producer if they were sent from a previous run.  (We are assuming we are "
                        "the only Producer")
    p.add_argument('-s', '--source',  default=SRC_BOOTSTRAP_SERVERS,
                   help=f"Source bootstrap server.  Defaults to {SRC_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g1', '--group-id1', default=SRC_GROUP_ID,
                   help=f"Source group id.  Defaults to {SRC_GROUP_ID} if not specified.")
    p.add_argument('-a', '--target',  default=TRG_BOOTSTRAP_SERVERS,
                   help=f"Target bootstrap server.  Defaults to {TRG_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g2', '--group-id2', default=TRG_GROUP_ID,
                   help=f"Target group id.  Defaults to {TRG_GROUP_ID} if not specified."),
    p.add_argument('-n', '--partitions', default=TARGET_PARTITION_COUNT,
                   help=f"Number of target partitions to use. Defaults to {TARGET_PARTITION_COUNT}. "
                        "If the source topic has 16 partitions and the target has 32 partitions, each consumer "
                        "will produce to 2 partitions.  This way, a later consumer can use 32 threads instead of 16.")
    args = p.parse_args()

    replicate(topic=args.topic,
              reset=args.reset,
              source=args.source,
              src_groupid=args.group_id1,
              target=args.target,
              trg_groupid=args.group_id2,
              trg_partitions=args.partitions
              )
