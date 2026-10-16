import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType


def create_spark():
    version = pyspark.__version__
    scala = '2.13' if int(version.split('.')[0]) >= 4 else '2.12'
    spark = (SparkSession.builder.appName('WikiStreams')
             .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2')
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


schema = StructType([
    StructField('title', StringType()),
    StructField('user', StringType()),
    StructField('timestamp', LongType()),
    StructField('type', StringType()),
    StructField('wiki', StringType()),
    StructField('server_name', StringType()),
    StructField('bot', BooleanType()),
    StructField('minor', BooleanType()),
    StructField('length', StructType([
        StructField('old', LongType()), StructField('new', LongType())
    ])),
])


def read_events(spark):
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:19092'))
           .option('subscribe', 'wikistreams')
           .option('startingOffsets', 'earliest').load())
    return (raw.select(F.from_json(F.col('value').cast('string'), schema).alias('event'))
            .select('event.*').filter((F.col('type') == 'edit') & F.col('timestamp').isNotNull())
            .withColumn('event_time', F.timestamp_seconds('timestamp'))
            .withColumn('edit_delta', F.col('length.new') - F.col('length.old'))
            .withColumn('edit_size', F.abs('edit_delta'))
            .withWatermark('event_time', '2 minutes'))


def aggregations(events):
    return {
        'bot_edits_1h': events.groupBy(F.window('event_time', '1 hour'), 'bot')
            .agg(F.count('*').alias('edit_count')),
        'tumbling_edits_1m': events.groupBy(F.window('event_time', '1 minute'), 'wiki', 'bot')
            .agg(F.count('*').alias('edit_count'),
                 F.sum('edit_delta').alias('net_bytes'),
                 F.sum('edit_size').alias('changed_bytes'),
                 F.round(F.avg('edit_size'), 2).alias('avg_edit_bytes')),
        'sliding_edits_2m': events.groupBy(F.window('event_time', '2 minutes', '30 seconds'), 'wiki')
            .agg(F.count('*').alias('edit_count'),
                 F.sum('edit_size').alias('changed_bytes'),
                 F.max('edit_size').alias('largest_edit_bytes')),
        'large_page_edits': events.filter(F.col('edit_size') >= 500)
            .groupBy(F.window('event_time', '1 minute'), 'title')
            .agg(F.count('*').alias('edit_count'), F.sum('edit_size').alias('changed_bytes')),
    }


def start_queries(frames, checkpoint_root='/home/jovyan/data/checkpoints'):
    queries = []
    for name, frame in frames.items():
        query = (frame.writeStream.queryName(name).format('console').outputMode('update')
                 .option('truncate', False).option('numRows', 30)
                 .option('checkpointLocation', f'{checkpoint_root}/{name}')
                 .trigger(processingTime='10 seconds').start())
        queries.append(query)
    return queries


if __name__ == '__main__':
    spark = create_spark()
    queries = start_queries(aggregations(read_events(spark)))
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()
