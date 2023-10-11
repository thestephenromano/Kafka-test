import time

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, Producer, Consumer, KafkaError

# Kafka broker address
broker_list = 'localhost:29101,localhost:29102,localhost:29103,localhost:29104,localhost:29105'

# Create an AdminClient instance
admin_config = {
    'bootstrap.servers': broker_list,
}
admin_client = AdminClient(admin_config)

#
# Bootstrap Topics
#

TOPIC_NAME = "new_user"

# perform any cleanup to start fresh
delete_topics_future = admin_client.delete_topics([TOPIC_NAME])
try:
    delete_topics_future[TOPIC_NAME].result()
    print(f"Topic: {TOPIC_NAME} marked for deletion...")
    time.sleep(10)  # topic is marked for deletion, give Kafka a few seconds to clean complete
except Exception as e:
    print(f"Failed to delete topic {TOPIC_NAME}: {e}")

# create topic
new_topic = NewTopic(
    topic=TOPIC_NAME,
    num_partitions=3,
    replication_factor=1,
)

# Create the topic
create_topics_future = admin_client.create_topics([new_topic])
create_topics_future[TOPIC_NAME].result()
print(f"Topic: {TOPIC_NAME} created")

#
# Produce messages
#

producer_config = {
    'bootstrap.servers': broker_list,
}
producer1 = Producer(producer_config)
producer2 = Producer(producer_config)

KEY = "00000000-0000-0000-0000-000000000000"  # key is used to determine partition, messages are ordered by partition

print("Producing message. topic: new_user, key: 00000000-0000-0000-0000-000000000000, value: peter")
producer1.produce(TOPIC_NAME, key=KEY, value="peter")
producer1.flush()
print("Producing message. topic: new_user, key: 00000000-0000-0000-0000-000000000000, value: stephen")
producer2.produce(TOPIC_NAME, key=KEY, value="stephen")
producer2.flush()

#
# Consume messages
#
consumer_config = {
    'bootstrap.servers': broker_list,
    'group.id': 'group-1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)
# Subscribe to the topic
consumer.subscribe([TOPIC_NAME])

# Poll for new messages for 30 seconds
now = time.time()
end = now + 30

try:
    while time.time() < end:
        msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}, offset {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            print(f"Consumed message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()
