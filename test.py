from confluent_kafka import Consumer, Producer
import time
import os

#Loading env variables
from dotenv import load_dotenv
load_dotenv()

# Kafka configuration
destination_topic = os.environ.get('DESTINATION_TOPIC')
group_id = 'my_consumer_group'
bootstrap_local = os.environ.get('BOOTSTRAP_LOCAL')

# Confluent Cloud configuration
source_topic = os.environ.get('SOURCE_TOPIC_CONFLUENT')
cloud_bootstrap_servers = os.environ.get('CLOUD_BOOTSTRAP_SERVERS')
cloud_api_key = os.environ.get('CLOUD_API_KEY')
cloud_api_secret = os.environ.get('CLOUD_API_SECRET')
cloud_group_id = 'cloud_consumer_group'

# Create a Kafka consumer
consumer_conf = {
    'bootstrap.servers': cloud_bootstrap_servers,
    'group.id': cloud_group_id,
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': cloud_api_key,
    'sasl.password': cloud_api_secret
}
consumer = Consumer(consumer_conf)
consumer.subscribe([source_topic])

# Create a Kafka producer
producer_conf = {
    'bootstrap.servers': bootstrap_local
}
producer = Producer(producer_conf)

try:
    while True:
        # Poll for a message from the source topic
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # Wait for five seconds
        time.sleep(5)

        # Stream the message to the destination topic
        producer.produce(destination_topic, key=msg.key(), value=msg.value(), partition=msg.partition())
        producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
