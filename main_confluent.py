#
#  Old Code: could possibly be reused once confluent provides support for getting the ending offset per partition
#   so that get_topic_offset_sum works.  Without it we cannot tell if we got all messages:  though kafka streams
#   are treated infinite, for ZTF they are not and end every night.  We want to be sure we get all of the messages
#   so confluent's lack of getting ending offset (not the same as highwater marks) makes it not as well suitable for
#   our purposes.
#  So we are leaving this code in hopes of one day confluent provides support. Confluent's library is supposedly
#  faster and we would like to benefit from that one day.




import sys
import logging
import argparse
import time
import datetime
import multiprocessing as mp
#import kafka # kafka-python package
import lz4  # Not necessary to import lz4, but it does need to be installed for the kafka module
from kafka.admin import KafkaAdminClient, NewTopic
import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
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

source_partitions = None


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


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.error('Message delivery failed: {}'.format(err))
    else:
        pass
        # print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def proc_replicate(topic, src_partition, end_offset, part_map, rerun=False, commit=False):
    """
      part_map list[list[]]
    """

    src = Consumer(
        {'bootstrap.servers': SRC_BOOTSTRAP_SERVERS,
         'group.id': SRC_GROUP_ID,
         'enable.auto.commit': commit
         })

    logger.info(f"Starting process consumer topic: {topic} src_partition:{src_partition}")

    if rerun:
        logger.info(f"Resetting source partition {src_partition} to beginning...")
        tp = TopicPartition(topic, src_partition, confluent_kafka.OFFSET_BEGINNING)
        src.assign([tp])
        src.seek(tp)
        logger.info(f"Reset of source partition {src_partition} offset to {src.position([tp])} complete.")
    else:
        tp = TopicPartition(topic, src_partition)
        src.assign([tp])

    trg = Producer({'bootstrap.servers': TRG_BOOTSTRAP_SERVERS,
                    'group.id': TRG_GROUP_ID})

    trg_part_ndx = 0
    trg_part_ndx_max = len(part_map[src_partition])-1  # ex: a length of 2 has 1 as the max

    msg_count = 0

    t0 = _t1 = time.time()
    ending_offset = end_offset

    cd = 30.0
    while True:
        st = time.time()
        msg = src.poll(1.0)

        if msg is None:
            if time.time() - st >= cd:
                logger.info(f"timeout after {cd} secs for topic: {topic} src_partition:{src_partition}, ending")
                break
            continue

        if msg.error():
            logger.error(f"Consumer error topic: {topic} src_partition:{src_partition}: {msg.error()} exiting")
            sys.exit(1)

        msg_count += 1
        trg.produce(topic, value=msg.value(), partition=part_map[src_partition][trg_part_ndx])

        if commit: src.commit()

        # 300 secs, we must do this if we want to ensure not to lose messages
        # learned of during testing; w/o it, messages do get produced but many, many
        # will not show up in the target cluster
        trg.flush(300)
        trg_part_ndx += 1
        if trg_part_ndx > trg_part_ndx_max:
            trg_part_ndx = 0

        # Print status/stats
        if msg_count % LOG_INTERVAL == 0:
            _t1 = outputstat(t0, _t1, LOG_INTERVAL, src_partition, msg.offset, msg_count, ending_offset)

    logger.info(f"process consumer, source partition {src_partition} replication complete ========================")
    _t1 = outputstat(t0, _t1, LOG_INTERVAL, src_partition, -1, msg_count, ending_offset)


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


def get_assignment(topics, partitions):
    global source_partitions
    print("Assignment:", partitions)
    source_partitions = partitions


def replicate(topic, rerun, delete, source, src_groupid, target, trg_groupid, trg_partitions):
    global source_partitions

    # Connect to source kafka cluster
    src = Consumer({'bootstrap.servers': source,
                    'group.id': src_groupid,
                    'auto.offset.reset': 'smallest',
                    'enable.auto.commit': False
                    })

    # Connect to target kafka cluster
    trg = Consumer({'bootstrap.servers': target,
                    'group.id': trg_groupid,
                   })

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

    src_cm = src.list_topics()  # returns ClusterMetadata
    if the_topic not in src_cm.topics:
        logger.error(f"Current topics in {source} with group id {src_groupid} are:")
        logger.error(f"{src_cm.topics}")
        logger.error(f"Topic {topic} not in cluster {source} with group id {src_groupid}")
        sys.exit(1)

    src_partition_count = len(src_cm.topics[the_topic].partitions)

    logger.info(f"topic: {the_topic} has # of partitions: {src_partition_count}")
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

    for part in src_cm.topics[the_topic].partitions:
        tp = TopicPartition(the_topic, part)
        tp.offset = confluent_kafka.OFFSET_BEGINNING
        src.assign([tp])
        any_committed = src.committed([tp])
        committed = any_committed[0].offset
        total_committed += committed
        end_offset = src.get_watermark_offsets(tp, cached=False)[1]
        position = src.position([tp])[0].offset
        if position == confluent_kafka.OFFSET_BEGINNING:
            position = 0
        elif position == confluent_kafka.OFFSET_END:
            position = end_offset
        elif position == confluent_kafka.OFFSET_INVALID:
            position = 0

        parts[str(part)] = end_offset
        total_offsets += end_offset
        logger.info("Source topic: %s partition: %s end offset: %s committed: %s position: %s lag: %s" %
                    (the_topic, part, end_offset, committed, position, (position - committed)))

    src.close()
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

    logger.info(f"END")

    # Use to debug replication of one partition; it's easier to stepwise debug in pycharm using this
    #proc_replicate(the_topic, 1, parts["0"], part_map, rerun)


def get_topic_offset_sum(topic, cluster):
    topic_offsets_sum = 0
    cm = cluster.list_topics(topic)

    for part in cm.topics[topic].partitions:
        tp = TopicPartition(topic, part, confluent_kafka.OFFSET_END)
        cluster.assign([tp])

        tp = TopicPartition(topic, part)
        # high_offset is last message read not last message in the parition
        # so we cannot use this!
        # Leaving this here in case confluent provides a method to get the ending offset per partition
        low_offset, high_offset = cluster.get_watermark_offsets(tp, cached=False)
        topic_partitions = cluster.position([tp])
        topic_offsets_sum += high_offset

    return topic_offsets_sum


def determine_topic(topic, src, trg, rerun):

    offset_sum_delta = DOESNT_EXIST      # indicates target topic doesn't exist
    trg_topics = []
    trg_topics = list(trg.list_topics().topics)
    trg_topics.sort(key=lambda x: x[5:13], reverse=True)

    if topic:
        if rerun:
            the_topic = topic
        else:
            trg_offset_sum = get_topic_offset_sum(topic, trg)
            src_offset_sum = get_topic_offset_sum(topic, src)
            logger.info(f"trg_offset_sum={trg_offset_sum} src_offset_sum={src_offset_sum}")

            # is the topic in the target and is the sum of end offsets smaller than the source?
            if trg_offset_sum < src_offset_sum:
                the_topic = topic
                offset_sum_delta = src_offset_sum - trg_offset_sum
            else:
                logger.info("Target sum of ending offsets of each partition is not less than the Source")
                logger.info(f"Use the rerun option to reload the topic {topic}")
                sys.exit(0)

    else:
        # automatically determine the topic to load
        src_topics_set = src.topics()
        src_topics = list(src_topics_set)
        src_topics.sort(key=lambda x: x[5:13], reverse=True)
        print_source_topics(src_topics)
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
