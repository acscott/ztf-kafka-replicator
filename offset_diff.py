import sys
import logging
import datetime
import argparse
import time
import multiprocessing as mp
import kafka # kafka-python package
import lz4  # Not necessary to import lz4, but it does need to be installed for the kafka module
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata

SRC_GROUP_ID = 'antares_junction'
SRC_BOOTSTRAP_SERVERS = 'public.alerts.ztf.uw.edu:9092'

TRG_GROUP_ID = 'ztf'
TRG_BOOTSTRAP_SERVERS = 'localhost:9092'


def offset_diff(source, src_groupid, target, trg_groupid):

    # Connect to source kafka cluster
    src = kafka.KafkaConsumer(group_id=src_groupid,
                              bootstrap_servers=source)
    # Connect ot target kafka cluster
    trg = kafka.KafkaConsumer(group_id=trg_groupid,
                              bootstrap_servers=target)

    src_topics_set = src.topics()
    src_topics = list(src_topics_set)
    src_topics.sort()

    trg_topics_set = trg.topics()
    trg_topics = list(trg_topics_set)
    trg_topics.sort()
    w = 50
    sndx = 0
    smax = len(src_topics)
    tndx = 0
    tmax = len(trg_topics)
    endofs = False
    endoft = False
    print(datetime.datetime.now().isoformat())
    print(source.ljust(w) + ' | ' + target.ljust(w))
    print(src_groupid.ljust(w) + ' | ' + trg_groupid.ljust(w))
    print('-' * w + '---' + '-' * w)
    while True:
        if sndx < smax:
            s = src_topics[sndx]
        else:
            s = ''
            endofs = True

        if tndx < tmax:
            t = trg_topics[tndx]
        else:
            t = ''
            endoft = True

        if endofs and endoft: break

        if t < s and not endoft:
            t_offset_sum = get_topic_offset_sum(t, trg)
            print(''.ljust(w) + ' | ' + (t[0:w] + ':' + t_offset_sum).ljust(w))
            tndx += 1
        elif t == s:
            t_offset_sum = get_topic_offset_sum(t, trg)
            s_offset_sum = get_topic_offset_sum(s, src)
            t_delta = s_offset_sum - t_offset_sum
            t_pcnt = t_offset_sum / s_offset_sum
            t_pctn_str = "{:.2%}".format(t_pcnt)
            s_position_sum = get_topic_position_sum(s, trg)
            if s_position_sum == -999: s_position_sum = ''
            if t_delta == "0":
                t_delta = str("COMPLETE")
            else:
                t_delta = ' Δ=' + str(t_delta) + ' ' + t_pctn_str
            s_offset_n_commit = str(s_offset_sum) + '(' + str(s_position_sum) + ')'
            print((s[0:w] + ' ' + s_offset_n_commit).ljust(w) + ' | ' + (t[0:w] + ':' + str(t_offset_sum)).ljust(w) +
                  t_delta)
            tndx += 1
            sndx += 1
        else:
            if not endofs:
                s_offset_sum = str(get_topic_offset_sum(s, src))
                s_position_sum = get_topic_position_sum(s, trg)
                if s_position_sum == -999: s_position_sum = ''
                s_offset_n_commit = str(s_offset_sum) + '(' + str(s_position_sum) + ')'
                print((s[0:w] + ' ' + s_offset_n_commit).ljust(w) + ' | ' + ''.ljust(w))
                sndx += 1
    print('=' * w + '===' + '=' * w)


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


def get_topic_position_sum(topic, cluster):
    topic_position_sum = 0
    try:
        for part in cluster.partitions_for_topic(topic):
            tp = TopicPartition(topic, part)
            cluster.assign([tp])
            c = cluster.position(tp)
            # c is None if there are no commits for the topic partition (c is actually the offset that will be used,
            # semantically slightly different than the number of commits)
            if c:
                topic_position_sum += c
            o = cluster.committed(tp)
            if o:
                print('.')
    except TypeError:
        # The cluster has no partitions of the topic
        return -999

    return topic_position_sum


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='offset-diff',
                                description="")
    p.add_argument('-s', '--source',  default=SRC_BOOTSTRAP_SERVERS,
                   help=f"Source bootstrap server.  Defaults to {SRC_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g1', '--group-id1', default=SRC_GROUP_ID,
                   help=f"Source group id.  Defaults to {SRC_GROUP_ID} if not specified.")
    p.add_argument('-a', '--target',  default=TRG_BOOTSTRAP_SERVERS,
                   help=f"Target bootstrap server.  Defaults to {TRG_BOOTSTRAP_SERVERS} if not specified.")
    p.add_argument('-g2', '--group-id2', default=TRG_GROUP_ID,
                   help=f"Target group id.  Defaults to {TRG_GROUP_ID} if not specified."),
    args = p.parse_args()

    offset_diff(
              source=args.source,
              src_groupid=args.group_id1,
              target=args.target,
              trg_groupid=args.group_id2
              )