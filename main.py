import datetime
import time
import multiprocessing as mp
import curses
from curses import wrapper
import kafka #kafka-python package
import lz4  # Not necesssary to import lz4, but it does need to be installed for kafka
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


def outputstat(t1, interval, partition, offset, count, ending_offset):

    pcntcomplete = "{:.2%}".format(count / ending_offset)
    t2 = time.time()
    mps = interval / (t2 - t1)
    t1 = t2
    mpsf = "{:.2f}".format(mps)
    leftsecs = (ending_offset - count) / mps
    m, s = divmod(int(leftsecs), 60)
    h, m = divmod(m, 60)

    print(f"\033[F{partition}:{offset} - {mpsf} mps, {pcntcomplete} eta:" +
          '{:d}h:{:02d}m:{:02d}s'.format(h, m, s))
    return t1


def proc_replicate(topic, src_partition, end_offset, part_map, reset=False):
    """
      part_map list[list[]]
    """

    src = kafka.KafkaConsumer(group_id=SRC_GROUP_ID,
                              bootstrap_servers=SRC_BOOTSTRAP_SERVERS,
                              auto_offset_reset='earliest')
    print(f"topic: {topic} part:{src_partition}")
    tp = TopicPartition(topic, src_partition)
    src.assign([tp])
    if reset:
        print(f"Resetting source partition {src_partition} to beginning")
        src.seek_to_beginning(tp)

    trg = kafka.KafkaProducer(client_id=TRG_GROUP_ID,
                              bootstrap_servers=TRG_BOOTSTRAP_SERVERS)
    trg_part_ndx = 0
    trg_part_ndx_max = len(part_map[src_partition])-1  # a length of 2 has 1 as the max

    count = 0
    interval = 150
    t0 = t1 = time.time()
    ending_offset = end_offset[list(end_offset)[0]]
    for msg in src:
        count += 1
        trg.send(topic, value=msg.value, partition=part_map[src_partition][trg_part_ndx])
        trg_part_ndx += 1
        if trg_part_ndx > trg_part_ndx_max:
            trg_part_ndx = 0

        # Print status/stats
        if count % interval == 0:
            t1 = outputstat(t1, interval, src_partition, msg.offset, count, ending_offset)

            #stdscr.addstr(partition, 1, msg.offset)

        #print(f"part={msg.partition} key={msg.key} offset={msg.offset}")

    #curses.endwin()


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


def replicate(reset):

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

    # Print list of topics
    i = 0
    for item in src_topics:
        print(f"{i}:{item}")
        i = i+1

    # Determine if latest source topic is at least partially loaded to target
    the_topic = 'ztf_20210307_programid1'

    part_set = src.partitions_for_topic(the_topic)
    src_partition_count = len(part_set)
    print(f"topic: {the_topic} # of partitions: {src_partition_count}")
    # Calculate multiplier for demuxing
    # Example:
    #    source = 4 target = 9 then multiplier is 9/4=2.25
    #    int(2.25) = 2
    multiplier = int(TARGET_PARTITION_COUNT / src_partition_count)
    trg_partition_count = src_partition_count * multiplier
    print(f"multiplier={multiplier} target_partition_coun2={trg_partition_count}")
    # Add the new topic in target cluster
    if the_topic not in trg_topics:
        print(f"replicate {the_topic} to {KAFKA_JUNCTION}")
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
    print("=========================================================================")
    parts = {}
    for p in src.partitions_for_topic(the_topic):
        tp = TopicPartition(the_topic, p)
        src.assign([tp])
        any_committed = src.committed(tp)
        committed = any_committed if any_committed else 0
        #consumer.seek_to_end(tp)
        last_offset = src.position(tp)
        end_offset = src.end_offsets([tp])
        parts[str(p)] = end_offset

        print("topic: %s partition: %s end offset: %s committed: %s last: %s lag: %s" %
              (the_topic, p, end_offset[tp], committed, last_offset, (last_offset - committed)))
        #consumer.seek_to_beginning(tp)

    src.close(autocommit=False)

    procs = [mp.Process(target=proc_replicate,
                        args=(the_topic, p, parts[str(p)], part_map, reset)
                        ) for p in range(1, src_partition_count)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()


if __name__ == '__main__':
    replicate(reset=True)
