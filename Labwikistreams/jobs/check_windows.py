from wikistream_consumer import create_spark, aggregations
from pyspark.sql import functions as F

spark = create_spark()
try:
    events = spark.createDataFrame([
        (1780000010, 'frwiki', False, 'Test page', -600, 600),
    ], 'timestamp long, wiki string, bot boolean, title string, edit_delta long, edit_size long')
    events = events.withColumn('event_time', F.timestamp_seconds('timestamp'))
    frames = aggregations(events)
    tumbling = frames['tumbling_edits_1m'].collect()
    sliding = frames['sliding_edits_2m'].collect()
    large = frames['large_page_edits'].collect()
    assert len(tumbling) == 1
    assert tumbling[0].edit_count == 1
    assert tumbling[0].net_bytes == -600
    assert tumbling[0].changed_bytes == 600
    assert len(sliding) == 4
    assert all(row.edit_count == 1 for row in sliding)
    assert len(large) == 1 and large[0].changed_bytes == 600
    print('PASS: one tumbling window, four sliding windows, size and page filter')
finally:
    spark.stop()
