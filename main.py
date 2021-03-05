import kafka #kafka-python package
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer, TopicPartition


#KAFKA_JUNCTION = 'kafka-junction-kafka-bootstrap:9092'
KAFKA_JUNCTION = 'localhost:9092'


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def replicate(bootstrap_source):
    GROUP_ID='antares_staging'
    src = kafka.KafkaConsumer(group_id=GROUP_ID, bootstrap_servers=[bootstrap_source])
    # example: ztf_20200102programid1
    src_topics_set = src.topics()
    src_topics = list(src_topics_set)
    src_topics.sort(key=lambda x: x[5:13], reverse=True)

    trg = kafka.KafkaConsumer(group_id='ztf', bootstrap_servers=[KAFKA_JUNCTION])
    trg_topics_set = trg.topics()
    trg_topics = list(trg_topics_set)
    trg_topics.sort(key=lambda x: x[5:13], reverse=True)

    the_topic = src_topics[1]
    if the_topic not in trg_topics:
        print(f"replicate {the_topic} to {KAFKA_JUNCTION}")
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id='test'
        )
        topic_list = [NewTopic(name=the_topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_source,
        group_id=GROUP_ID,
        enable_auto_commit=False
    )
    for p in consumer.partitions_for_topic(the_topic):
        tp = TopicPartition(the_topic, p)
        consumer.assign([tp])
        any_committed = consumer.committed(tp)
        committed = any_committed if any_committed else 0
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)
        print("topic: %s partition: %s committed: %s last: %s lag: %s" % (the_topic, p, committed, last_offset,
                                                                          (last_offset - committed)))

    consumer.close(autocommit=False)


if __name__ == '__main__':
    print_hi('PyCharm')
    replicate('public.alerts.ztf.uw.edu:9092')
