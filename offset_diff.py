import sys
import logging
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
    w = 40
    sndx = 0
    smax = len(src_topics)
    tndx = 0
    tmax = len(trg_topics)
    endofs = False
    endoft = False
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
            print(''.ljust(w) + ' | ' + t[0:w].ljust(w))
            tndx += 1
        elif t == s:
            print(s[1:w].ljust(w) + ' | ' + t[0:w].ljust(w))
            tndx += 1
            sndx += 1
        else:
            if not endofs:
                print(s[1:w].ljust(40) + ' | ' + ''.ljust(w))
                sndx += 1




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