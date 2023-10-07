from pyflink.table.expressions import *
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf
import requests


# Define a Python function to calculate distance using the Google Maps API
def calculate_eta(lat, lon):
    api_key = "INSERT_YOUR_API_KEY"
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins={lat},{lon}&destinations=28.5017325,77.1672616&key={api_key}"
    response = requests.get(url)
    data = response.json()
    duration_text = data["rows"][0]["elements"][0]["duration"]["text"]
    return duration_text


# Define a PyFlink UDF for distance calculation
@udf(
    result_type=DataTypes.STRING(),
    input_types=[DataTypes.FLOAT(), DataTypes.FLOAT()],
)
def calculate_eta_udf(lat, lon):
    return calculate_eta(lat, lon)


def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # t_env.get_config().set("pipeline.jars", "file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")
    t_env.get_config().set(
        "pipeline.jars",
        "file:///Users/karanbawejapro/Desktop/jarfiles/flink-sql-connector-kafka-1.17.1.jar",
    )
    t_env.get_config().set("table.exec.source.idle-timeout", "1000")

    # Register UDF using this --> check Slack for troubleshooting
    t_env.create_temporary_system_function("calc_eta", calculate_eta_udf)

    # For Confluent Cloud connection to Flink --> check Slack for troubleshooting
    # JAAS config and other configs as per the source DDL below below

    # Local Kafka SOURCE conn
    source_ddl = """
        CREATE TABLE gps_coords (
            lat FLOAT,
            long FLOAT,
            ts_coord BIGINT,
            clientId STRING,
            ts_ltz AS TO_TIMESTAMP_LTZ(ts_coord, 3),
            WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mqtt_source',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json',
            'properties.group.id' = 'coords_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
    # Confluent Cloud SOURCE conn
    # source_ddl = """
    #     CREATE TABLE gps_coords(
    #         lat FLOAT,
    #         long FLOAT,
    #         ts_coord BIGINT,
    #         clientId STRING,
    #         ts_ltz AS TO_TIMESTAMP_LTZ(ts_coord,3),
    #         WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECONDS
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = 'mqtt-data',
    #         'properties.bootstrap.servers' = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    #         'properties.group.id' = 'coords_group',
    #         'scan.startup.mode' = 'specific-offsets',
    #         'scan.startup.specific-offsets' = 'partition:0,offset:0',
    #         'json.fail-on-missing-field' = 'false',
    #         'json.ignore-parse-errors' = 'true',
    #         'format' = 'json',
    #         'properties.security.protocol' = 'SASL_SSL',
    #         'properties.sasl.mechanism' = 'PLAIN',
    #         'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="L55O2A25JWO7XUKI" password="8LUXrAJnTsFV4y2C55VuHWUrgIkkzZDNvZ7rbVncNOW8iHKCBcfD50b51LLMLTQJ";'
    #     )
    #     """

    window_sql = """
    INSERT INTO sink_kafka
    SELECT
        clientId,
        window_start,
        FIRST_VALUE(lat) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz) AS lat,
        FIRST_VALUE(long) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz) AS long,
        calc_eta(lat, long) AS ETA
    FROM (
        SELECT
            clientId,
            lat,
            long,
            window_start,
            window_end,
            ts_ltz
        FROM TABLE(
            HOP(TABLE gps_coords, DESCRIPTOR(ts_ltz), INTERVAL '5' SECONDS, INTERVAL '10' SECONDS)
        )
    ORDER BY window_start
    );
    """

    sink_kafka = """
            CREATE TABLE sink_kafka (
                clientId STRING,
                window_start TIMESTAMP,
                lat FLOAT,
                long FLOAT,
                ETA STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'mqtt_sink',
                'properties.bootstrap.servers' = 'localhost:9092',
                'format' = 'json'
            );
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_kafka)

    t_env.execute_sql(window_sql).wait()


if __name__ == "__main__":
    log_processing()


# Define the windowed aggregation query
# window_sql = """
#     INSERT INTO sink_kafka
#     SELECT
#         clientId,
#         window_start,
#         FIRST_VALUE(lat) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz) AS lat,
#         FIRST_VALUE(long) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz) AS long,
#         AVG(time_diff) AS avg_time,
#         AVG(distance) / AVG(time_diff) AS avg_speed
#     FROM (
#         SELECT
#             clientId,
#             lat,
#             long,
#             window_start,
#             window_end,
#             ts_ltz,
#             LAG(ts_ltz) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz) AS prev_ts_ltz,
#             TIMESTAMPDIFF(SECOND, LAG(ts_ltz) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz), ts_ltz) AS time_diff,
#             calculate_distance_udf(
#                 LAG(lat) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz),
#                 LAG(long) OVER (PARTITION BY clientId, window_start ORDER BY ts_ltz),
#                 lat,
#                 long
#             ) AS distance
#         FROM TABLE(
#             HOP(TABLE gps_coords, DESCRIPTOR(ts_ltz), INTERVAL '5' SECOND, INTERVAL '10' SECOND)
#         )
#     )
#     WHERE prev_ts_ltz IS NOT NULL
#     GROUP BY clientId, window_start
# """
