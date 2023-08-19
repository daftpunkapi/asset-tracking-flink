from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time

# Kafka configuration
source_topic = 'mqtt-geo-data'
destination_topic = 'mqtt-replay'
group_id = 'my_consumer_group'
bootstrap_local = 'localhost:9092'
admin_client = AdminClient({'bootstrap.servers': bootstrap_local})

new_topic = NewTopic(destination_topic, num_partitions = 6, replication_factor = 1)
admin_client.create_topics([new_topic])

# Confluent Cloud configuration
cloud_bootstrap_servers = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092'
cloud_group_id = 'cloud_consumer_group'
cloud_api_key = 'L55O2A25JWO7XUKI'
cloud_api_secret = '8LUXrAJnTsFV4y2C55VuHWUrgIkkzZDNvZ7rbVncNOW8iHKCBcfD50b51LLMLTQJ'

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
        time.sleep(2)

        # Stream the message to the destination topic
        producer.produce(destination_topic, key=msg.key(), value=msg.value(), partition=msg.partition())
        producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
