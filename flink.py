from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy, SimpleStringSchema
import logging, sys

def read_from_kafka(env):
    kafka_source = KafkaSource \
    .builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("mqtt-replay") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_group_id("my-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .build()

    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    env.add_source(kafka_source).print()
    env.execute()

if __name__ == '__main__':
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")
    # env.add_jars("file:///Users/karanbawejapro/Desktop/jarfiles/flink-sql-connector-kafka-1.17.1.jar")

    print("Start reading data from kafka: ")
    read_from_kafka(env)