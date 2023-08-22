from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time
import os

#Loading env variables
from dotenv import load_dotenv
load_dotenv()


# Local Kafka configuration
destination_topic = os.environ.get('DESTINATION_TOPIC')
bootstrap_local = os.environ.get('BOOTSTRAP_LOCAL')
admin_client = AdminClient({'bootstrap.servers': bootstrap_local})
group_id = 'local_consumer_group'

# Creating new topic in local Kafka cluster
new_topic = NewTopic(destination_topic, num_partitions = 6, replication_factor = 1)
admin_client.create_topics([new_topic])

# Confluent Cloud configuration
source_topic = os.environ.get('SOURCE_TOPIC_CONFLUENT')
cloud_bootstrap_servers = os.environ.get('CLOUD_BOOTSTRAP_SERVERS')
cloud_api_key = os.environ.get('CLOUD_API_KEY')
cloud_api_secret = os.environ.get('CLOUD_API_SECRET')
cloud_group_id = 'cloud_consumer_group_3'

# Create a Kafka consumer from Confluent Cloud
consumer_config = {
    'bootstrap.servers': cloud_bootstrap_servers,
    'group.id': cloud_group_id,
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': cloud_api_key,
    'sasl.password': cloud_api_secret
}
consumer = Consumer(consumer_config)
consumer.subscribe([source_topic])

# Create a Kafka producer to local cluster
producer_config = {
    'bootstrap.servers': bootstrap_local
}
producer = Producer(producer_config)

# Polling data from Cloud and sending it to local cluster every 2 seconds
try:
    while True:
        # Poll for a message from the source topic
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        # Wait for two seconds
        time.sleep(0.2)

        # Stream the message to the destination topic
        producer.produce(destination_topic, key=msg.key(), value=msg.value(), partition=msg.partition())
        producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()