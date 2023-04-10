import uuid
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

consumer = KafkaConsumer(
    # topic
    group_id=uuid.uuid4().hex,
    bootstrap_servers=[''],
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism='PLAIN',
    sasl_plain_username="",
    sasl_plain_password="",
    api_version=(0, 10, 1)
)
print('begin')
for message in consumer:
    print('begins')
    print("Topic:[%s] Partition:[%d] Offset:[%d] Value:[%s]" % (
        message.topic, message.partition, message.offset, message.value))
    print('end')
