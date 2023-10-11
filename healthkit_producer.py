import msgpack
from confluent_kafka import Producer

from data import generate_patients

TOPIC_NAME = "health_kit"

broker_list = 'localhost:29101,localhost:29102,localhost:29103,localhost:29104,localhost:29105'


producer_config = {
    'bootstrap.servers': broker_list,
}
producer = Producer(producer_config)


for patient in generate_patients(1000):
    print(f"Producing message. topic: {TOPIC_NAME}, key: {patient['patient_id']}, value: {patient}")
    producer.produce(TOPIC_NAME, key=patient['patient_id'], value=msgpack.packb(patient, use_bin_type=True))

producer.flush()