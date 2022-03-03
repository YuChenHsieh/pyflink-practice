import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.sinks import CsvTableSink
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka
from pyflink.table.window import Tumble
from pyflink.table.expressions import col

adgp_result_file = "/tmp/adgp_spent.csv"
camp_result_file = "/tmp/camp_spent.csv"


def init_logfile():
    if os.path.exists(adgp_result_file):
        os.remove(adgp_result_file)
    if os.path.exists(camp_result_file):
        os.remove(camp_result_file)


def register_table_source(st_env: StreamTableEnvironment):
    return st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("universal")
            .topic("adwin")
            # .start_from_earliest()
            .property("zookeeper.connect", "192.168.6.8:2181")
            .property("bootstrap.servers", "192.168.6.8:9092")
        ) \
        .with_format(  # declare a format for this system
            Json()
            .fail_on_missing_field(False)
            .schema(DataTypes.ROW([
                    DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
                    DataTypes.FIELD("action", DataTypes.STRING()),
                    DataTypes.FIELD("camp", DataTypes.STRING()),
                    DataTypes.FIELD("adgp", DataTypes.STRING()),
                    DataTypes.FIELD("adid", DataTypes.STRING()),
                    DataTypes.FIELD("winprice", DataTypes.FLOAT())
                    ])
                    )
        ) \
        .with_schema(  # declare the schema of the table
            Schema()
            .field("rowtime", DataTypes.TIMESTAMP(3))
            .rowtime(
                Rowtime()
                .timestamps_from_field("timestamp")
                .watermarks_periodic_bounded(60000))
            .field("action", DataTypes.STRING())
            .field("camp", DataTypes.STRING())
            .field("adgp", DataTypes.STRING())
            .field("adid", DataTypes.STRING())
            .field("winprice", DataTypes.FLOAT())

        ) \
        .create_temporary_table('source')


def register_adgp_sink(st_env: StreamTableEnvironment):

    return st_env.register_table_sink("result_adgp",
                                      CsvTableSink(["rowtime", "adgp", "camp", "spent"],
                                                   [DataTypes.TIMESTAMP(3),
                                                    DataTypes.STRING(),
                                                    DataTypes.STRING(),
                                                    DataTypes.FLOAT()
                                                    ],
                                                   adgp_result_file))


def register_camp_sink(st_env: StreamTableEnvironment):
    return st_env.register_table_sink("result_camp",
                                      CsvTableSink(["w", "camp", "spent"],
                                                   [DataTypes.TIMESTAMP(3),
                                                    DataTypes.STRING(),
                                                    DataTypes.FLOAT()
                                                    ],
                                                   camp_result_file))


if __name__ == '__main__':

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)

    st_env = StreamTableEnvironment.create(s_env)
    config = st_env.get_config().get_configuration()
    config.set_string(
        "pipeline.jars", 'file:///Users/veranza/SideProject/python-flink/jar_files/flink-sql-connector-kafka_2.11-1.13.0.jar'
    )

    init_logfile()

    register_table_source(st_env)
    register_adgp_sink(st_env)
    register_camp_sink(st_env)

    query = """
            select tumble_start(rowtime, interval '1' minute) as rowtime, adgp, camp, sum(winprice) as spent from source 
            group by tumble(rowtime, interval '1' minute), adgp, camp
            """
    adgp_spent_table = st_env.sql_query(query)
    adgp_spent_table.insert_into("result_adgp")

    adgp_spent_table.window(Tumble.over("1.minutes").on("rowtime").alias("w"))\
                    .group_by(col("w"), col("camp")) \
                    .select(col("w").start, col("camp"), col("spent").sum.alias("spent")) \
                    .insert_into("result_camp")

    st_env.execute("tumble time window streaming")
