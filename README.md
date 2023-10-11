# kafka-test

## Set up

###  Python Environment

    pip install -r requirements.txt

### Spin up Kafka cluster

    docker-compose up -d

## Execute the examples

### new_user

    python new_user.py

### health_kit

#### Start the consumer

    python healthkit_consumer.py

#### Start the producer

    python healthkit_producer.py