from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import *

def log_processing():

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("pipeline.jars", "file:///Users/Raghav/Desktop/DaftPunk/Resources/flink-sql-connector-kafka-1.17.1.jar")
    # t_env.get_config().set("pipeline.jars", "file:///Users/karanbawejapro/Desktop/jarfiles/flink-sql-connector-kafka-1.17.1.jar")
    t_env.get_config().set("table.exec.source.idle-timeout", "1000")

    
    source_ddl = """
        CREATE TABLE gps_coords(
            lat FLOAT,
            long FLOAT,
            ts_coord BIGINT,
            clientId STRING,
            ts_ltz AS TO_TIMESTAMP_LTZ(ts_coord,3),
            WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mqtt_mock',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'coords_group',
            'scan.startup.mode' = 'specific-offsets',
            'scan.startup.specific-offsets' = 'partition:0,offset:0',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'format' = 'json'
        )
        """    
    
    window_sql = """
    INSERT INTO sink_kafka
    SELECT
        clientId,
        FIRST_VALUE(lat) OVER (PARTITION BY window_start, window_end ORDER BY ts_ltz) AS lat,
        FIRST_VALUE(long) OVER (PARTITION BY window_start, window_end ORDER BY ts_ltz) AS long
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
    );
    """

    sink_kafka = """
            CREATE TABLE sink_kafka (
                clientId STRING,
                lat FLOAT,
                long FLOAT
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
    
if __name__ == '__main__':
    log_processing()