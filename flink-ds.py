from pyflink.common.typeinfo import Types
from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

import random

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(3)

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

# settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# t_env = StreamTableEnvironment.create(env, environment_settings=settings)
# t_env.get_config().set("pipeline.jars", "file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")
# t_env.get_config().set("table.exec.source.idle-timeout", "1000")

env.execute()