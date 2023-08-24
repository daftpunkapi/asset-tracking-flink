from pyflink.common.typeinfo import Types
from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars("file:///Users/karanbawejapro/Desktop/jarfiles/flink-sql-connector-kafka-1.17.1.jar")
env.add_jars("file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")

# deserialization_schema = (
#     JsonRowDeserializationSchema.builder().type_info(
#         type_info=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING()])
#     )
#     # .ignore_parse_errors()
#     .build()
# )

kafka_consumer = FlinkKafkaConsumer(
    topics="mqtt-replay",
    deserialization_schema=SimpleStringSchema(),
    properties={
        "bootstrap.servers": "localhost:9092",
        "group.id": "test_group8",
        "auto.offset.reset": "earliest",
    },
)

ds = env.add_source(kafka_consumer)
ds.print()
# env.set_parallelism(1)
env.execute()