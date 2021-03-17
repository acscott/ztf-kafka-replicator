import sys
import logging
import argparse
import time
import datetime
import multiprocessing as mp
import kafka # kafka-python package
import lz4  # Not necessary to import lz4, but it does need to be installed for the kafka module
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata


logger = logging.getLogger('ztf-kafka-replicator')
logger.setLevel(level=logging.DEBUG)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter('%(asctime)s %(levelname)s %(lineno)d:%(filename)s(%(process)d) - %(message)s')
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

DOESNT_EXIST = -1           # indicates topic does not exist in target broker

# Number of partitions to replicate to
TARGET_PARTITION_COUNT = 32

SRC_GROUP_ID = 'antares_junction'
SRC_BOOTSTRAP_SERVERS = 'public.alerts.ztf.uw.edu:9092'

TRG_GROUP_ID = 'ztf'
TRG_BOOTSTRAP_SERVERS = 'localhost:9092'

# Number of messages each thread produces between outputting stats in the log
LOG_INTERVAL = 250

# Time to wait until no more messages
CONSUMER_TIMEOUT = 300000  #ms


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


def proc_replicate(topic, src_partition, end_offset, part_map, rerun=False):
    """
      part_map list[list[]]
    """

    # src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
    #                           bootstrap_servers=SRC_BOOTSTRAP_SERVERS,
    #                           auto_offset_reset='smallest',
    #                           consumer_timeout_ms=CONSUMER_TIMEOUT,
    #                           enable_auto_commit=True)

    src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
                              bootstrap_servers=SRC_BOOTSTRAP_SERVERS,
                              auto_offset_reset='smallest',
                              consumer_timeout_ms=CONSUMER_TIMEOUT,
                              enable_auto_commit=False
                              )
    logger.info(f"Starting process consumer topic: {topic} src_partition:{src_partition}")
    tp = TopicPartition(topic, src_partition)
    src.assign([tp])
    if rerun:
        logger.info(f"Resetting source partition {src_partition} to beginning")
        src.seek_to_beginning(tp)

    trg = kafka.KafkaProducer(client_id=TRG_GROUP_ID,
                              bootstrap_servers=TRG_BOOTSTRAP_SERVERS,
                              acks='all')
    trg_part_ndx = 0
    trg_part_ndx_max = len(part_map[src_partition])-1  # ex: a length of 2 has 1 as the max

    msg_count = 0

    t0 = t1 = time.time()
    ending_offset = end_offset[list(end_offset)[0]]

    try:
        for msg in src:
            msg_count += 1
            trg.send(topic, value=msg.value, partition=part_map[src_partition][trg_part_ndx])
            #src.commit()
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
        logging.error(f"source partition {src_partition} no messages in 300 seconds, ending")

    logger.info(f"process consumer, source partition {src_partition} replication complete ========================")
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


def replicate(topic, rerun, delete, source, src_groupid, target, trg_groupid, trg_partitions):

    # Connect to source kafka cluster
    src = kafka.KafkaConsumer(group_id=src_groupid,
                              bootstrap_servers=source)
    # Connect to target kafka cluster
    trg = kafka.KafkaConsumer(group_id=trg_groupid,
                              bootstrap_servers=target)

    admin_client = KafkaAdminClient(
        bootstrap_servers=TRG_BOOTSTRAP_SERVERS,
        client_id=TRG_GROUP_ID
    )

    if delete:
        logger.warning(f"DELETING topic {topic} on {TRG_BOOTSTRAP_SERVERS} as requested")
        admin_client.delete_topics([topic])
        logger.warning(f"DELETION of {topic} completed.")

    logger.info(f"source cluster: {source}  source group_id: {src_groupid}")
    logger.info(f"target cluster: {target}  target group_id: {trg_groupid}")
    # Determine if latest source topic is at least partially loaded to target
    trg_topics, the_topic, offset_sum_delta = determine_topic(topic, src, trg, rerun)

    part_set = src.partitions_for_topic(the_topic)
    src_partition_count = len(part_set)
    logger.info(f"topic: {the_topic} # of partitions: {src_partition_count}")
    # Calculate multiplier for demuxing
    # Example:
    #    source = 4 target = 9 then multiplier is 9/4=2.25
    #    int(2.25) = 2
    multiplier = int(trg_partitions / src_partition_count)
    trg_partition_count = src_partition_count * multiplier
    logger.info(f"multiplier={multiplier} target_partition_count={trg_partition_count}")

    # Add the new topic in target cluster
    if the_topic not in trg_topics:
        logger.info(f"replicate {the_topic} to {TRG_BOOTSTRAP_SERVERS} with source group id: {src_groupid}")

        topic_list = [NewTopic(name=the_topic, num_partitions=trg_partition_count, replication_factor=1)]
        try:
            logger.info(f"Creating topic {the_topic} with {trg_partition_count} partitions")
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except kafka.errors.TopicAlreadyExistsError:
            logger.info(f"Topic already exists in {TRG_BOOTSTRAP_SERVERS} ")
    part_map = create_part_map(src_partition_count, multiplier)

    # Get offset status for each partition
    logger.info(f"Source broker partitions for topic {the_topic}")
    logger.info("-------------------------------------------------------------------------")
    parts = {}
    total_committed = 0
    total_offsets = 0
    for part in src.partitions_for_topic(the_topic):
        tp = TopicPartition(the_topic, part)
        src.assign([tp])
        any_committed = src.committed(tp)
        committed = any_committed if any_committed else 0
        total_committed += committed
        last_offset = src.position(tp)
        end_offset = src.end_offsets([tp])
        parts[str(part)] = end_offset
        total_offsets += end_offset[tp]
        logger.info("Source topic: %s partition: %s end offset: %s committed: %s last: %s lag: %s" %
                    (the_topic, part, end_offset[tp], committed, last_offset, (last_offset - committed)))

    src.close(autocommit=False)
    logger.info(f"Source: total_committed={total_committed} total_offsets={total_offsets}")
    logger.info("=========================================================================")

    logger.info(f"Starting multi-process: the_topic={the_topic} rerun={rerun} src_partition_count={src_partition_count}")
    procs = [mp.Process(target=proc_replicate,
                        args=(the_topic, part, parts[str(part)], part_map, rerun)
                        ) for part in range(0, src_partition_count)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()


def get_topic_offset_sum(topic, cluster):
    topic_offsets_sum = 0

    try:
        for part in cluster.partitions_for_topic(topic):
            tp = TopicPartition(topic, part)
            cluster.assign([tp])
            end_offset = cluster.end_offsets([tp])
            topic_offsets_sum += end_offset[list(end_offset)[0]]
    except TypeError:
        # The cluster has no partitions of the topic
        return -1

    return topic_offsets_sum


def determine_topic(topic, src, trg, rerun):

    offset_sum_delta = DOESNT_EXIST      # indicates target topic doesn't exist
    trg_topics = []
    if topic:
        if not rerun:
            trg_offset_sum = get_topic_offset_sum(topic, trg)
            src_offset_sum = get_topic_offset_sum(topic, src)
            logger.info(f"trg_offset_sum={trg_offset_sum} src_offset_sum={src_offset_sum}")

            # is the topic in the target and is the sum of end offsets smaller than the source?
            if trg_offset_sum <= src_offset_sum:
                the_topic = topic
                offset_sum_delta = src_offset_sum - trg_offset_sum
            else:
                logger.info("Target sum of ending offsets of each partition is not less than the Source")
                logger.info(f"Use the rerun option to reload the topic {topic}")
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
            logger.info("No topics are missing in the target cluster, will look for incomplete topics ")
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
            logger.info("All candidate source topics have 0 offset sums. Nothing to replicate. ")
            sys.exit(0)
        else:
            logger.info(f"Automatic determination of topic: {the_topic} with {offset_sum} sum of ending offsets of all"
                        " partitions")
            trg_offset_sum = get_topic_offset_sum(the_topic, trg)
            src_offset_sum = get_topic_offset_sum(the_topic, src)
            logger.info(f"trg_offset_sum={trg_offset_sum} src_offset_sum={src_offset_sum} (-1 = topic does not "
                        "exist)")

    return trg_topics, the_topic, offset_sum_delta


def list_items_not_in(list1, list2):
    return list(set(list1) - set(list2))


def print_source_topics(src_topics):
    logger.info("Source topics")
    logger.info("--------------------------------------")
    i = 0
    for item in src_topics:
        logger.info(f"{i:003}:{item}")
        i = i + 1
    logger.info("======================================")


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='kafka-replicator',
                                description="Replicate and demux a topic's partitions from a source to target "
                                "Kafka cluster")
    p.add_argument('-t', '--topic',
                   help="The name of the source topic.  Will use the latest one indicated"
                        "by the date for ztf and not present in target if not specified.")
    p.add_argument('-r', '--rerun', action='store_true', default=False,
                   help="Forces the Consumer to start from the beginning.  There's a chance you will duplicate "
                        "messages to the Producer if they were sent from a previous run.  (We are assuming we are "
                        "the only Producer")
    p.add_argument('-d', '--delete', action='store_true', default=False,
                   help="Delete the target topic if it exists.  WARNING: you will lose data, so be sure you can "
                   "recover.")
    p.add_argument('-s', '--source',  default=SRC_BOOTSTRAP_SERVERS,
                   help=f"Source bootstrap server.  Defaults to {SRC_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g1', '--group-id1', default=SRC_GROUP_ID,
                   help=f"Source group id.  Defaults to {SRC_GROUP_ID} if not specified.")
    p.add_argument('-a', '--target',  default=TRG_BOOTSTRAP_SERVERS,
                   help=f"Target bootstrap server.  Defaults to {TRG_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g2', '--group-id2', default=TRG_GROUP_ID,
                   help=f"Target group id.  Defaults to {TRG_GROUP_ID} if not specified.")
    p.add_argument('-n', '--partitions', default=TARGET_PARTITION_COUNT,
                   help=f"Number of target partitions to use. Defaults to {TARGET_PARTITION_COUNT}. "
                        "If the source topic has 16 partitions and the target has 32 partitions, each consumer "
                        "will produce to 2 partitions.  This way, a later consumer can use 32 threads instead of 16.")
    args = p.parse_args()

    start_time = time.time()
    start_datetime = datetime.datetime.now().isoformat()
    replicate(topic=args.topic,
              rerun=args.rerun,
              delete=args.delete,
              source=args.source,
              src_groupid=args.group_id1,
              target=args.target,
              trg_groupid=args.group_id2,
              trg_partitions=args.partitions
              )
    total_time = time.time() - start_time
    m, s = divmod(int(total_time), 60)
    h, m = divmod(m, 60)
    print(f"Start @ {start_datetime}")
    print(f"End @ {datetime.datetime.now().isoformat()}")
    print(f"Total time " + '{:d}h:{:02d}m:{:02d}s'.format(h, m, s))
    print(f"consumer timeout is {CONSUMER_TIMEOUT} and is included in total")
