from pyflink.common.typeinfo import Types
from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
import random

env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars("file:///Users/karanbawejapro/Desktop/jarfiles/flink-sql-connector-kafka-1.17.1.jar")
env.add_jars("file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")


kafka_consumer = FlinkKafkaConsumer(
    topics="mqtt-replay",
    deserialization_schema=SimpleStringSchema(),
    properties={
        "bootstrap.servers": "localhost:9092",
        "group.id": f"mqtt-group-{random.randint(0, 999)}",
        "auto.offset.reset": "earliest",
    },
)

ds = env.add_source(kafka_consumer)
ds.print()
# env.set_parallelism(1)
env.execute()