import msgpack
from confluent_kafka import Consumer, KafkaError

TOPIC_NAME = "health_kit"

broker_list = 'localhost:29101,localhost:29102,localhost:29103,localhost:29104,localhost:29105'

consumer_config = {
    'bootstrap.servers': broker_list,
    'group.id': 'group-1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)
# Subscribe to the topic
consumer.subscribe([TOPIC_NAME])

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}, offset {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            # unpack the message
            patient = msgpack.unpackb(msg.value(), raw=False)
            print(f"Consumed message: {patient}")
except KeyboardInterrupt:
    pass
finally:
    # Close the Kafka consumer
    consumer.close()