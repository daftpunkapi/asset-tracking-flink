from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import time, logging, os, uuid, json

# Loading env variables
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Local Kafka configuration
destination_topic = os.environ.get("DESTINATION_TOPIC")
bootstrap_local = os.environ.get("BOOTSTRAP_LOCAL")
# group_id = 'local_consumer_group'

# Creating new topic in local Kafka cluster
admin_client = AdminClient({"bootstrap.servers": bootstrap_local})
new_topic = NewTopic(destination_topic, num_partitions=6, replication_factor=1)
admin_client.create_topics([new_topic])

# Confluent Cloud configuration
source_topic = os.environ.get("SOURCE_TOPIC_CONFLUENT")
cloud_bootstrap_servers = os.environ.get("CLOUD_BOOTSTRAP_SERVERS")
cloud_api_key = os.environ.get("CLOUD_API_KEY")
cloud_api_secret = os.environ.get("CLOUD_API_SECRET")
cloud_group_id = uuid.uuid1()

# Create a Kafka consumer from Confluent Cloud
consumer_config = {
    "bootstrap.servers": cloud_bootstrap_servers,
    "group.id": cloud_group_id,
    "auto.offset.reset": "earliest",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": cloud_api_key,
    "sasl.password": cloud_api_secret,
}

# Create a Kafka producer to local cluster
producer_config = {"bootstrap.servers": bootstrap_local}

producer = Producer(producer_config)

consumer = Consumer(consumer_config)
consumer.subscribe([source_topic])


# Polling data from Cloud and sending it to local cluster every 2 seconds
def main():
    try:
        logger.info(" Subscribed to source topic => %s", source_topic)

        while True:
            # Poll for a message from the source topic
            msg = consumer.poll(1.0)
            print(msg)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info("Reached end of partition")
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            # Stream the message to the destination topic
            try:
                print(type(json.dumps(msg.value().decode("utf-8"))))
                producer.produce(
                    destination_topic,
                    key=msg.key(),
                    value=json.dumps(msg.value().decode("utf-8")),
                    partition=msg.partition(),
                )
                producer.flush()
                logger.info(
                    "Produced message to partition %s, offset %s",
                    msg.partition(),
                    msg.offset(),
                )

                # Sleep for 2 seconds
                time.sleep(.1)

            except Exception as e:
                logger.error("Producer error: %s", str(e))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()