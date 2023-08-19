from confluent_kafka import Consumer
import time
import os

#Loading env variables
from dotenv import load_dotenv
load_dotenv()

# # Kafka configuration
# destination_topic = os.environ.get('DESTINATION_TOPIC')
# group_id = 'my_consumer_group'
# bootstrap_local = os.environ.get('BOOTSTRAP_LOCAL')

# Confluent Cloud configuration
source_topic = 'mqtt-geo-data'
cloud_bootstrap_servers = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092'
cloud_api_key = os.environ.get('CLOUD_API_KEY_2')
cloud_api_secret = os.environ.get('CLOUD_API_SECRET_2')
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
# consumer = Consumer(consumer_conf)
# consumer.subscribe([source_topic])

# Create a Kafka producer
# producer_conf = {
#     'bootstrap.servers': bootstrap_local
# }
# producer = Producer(producer_conf)

def consume_and_print():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([source_topic])
    print("loop started")
    try:
        while True:
            msg = consumer.poll(10)  # Poll for a message
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            
            # Print the consumed message's key and value
            print(f"Key: {msg.key()}, Value: {msg.value()}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_print()
