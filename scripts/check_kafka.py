"""Check that Python and Spark can read a sample Kafka message."""
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka3 import KafkaConsumer, KafkaProducer
from kafka3.admin import KafkaAdminClient, NewTopic
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


def check_kafka(spark, project_root, bootstrap="127.0.0.1:9092"):
    """Send one sample message, check both readers and remove the test topic."""
    topic = "a2-setup-" + uuid.uuid4().hex
    table = "a2_setup_" + uuid.uuid4().hex
    payload = [{"message": "environment-ready",
                "event_timestamp": datetime.now(timezone.utc).isoformat()}]
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
    producer = consumer = query = None
    created = False
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        created = True
        producer = KafkaProducer(bootstrap_servers=bootstrap,
                                 value_serializer=lambda value: json.dumps(value).encode("utf-8"))
        producer.send(topic, payload).get(timeout=15)
        producer.flush(timeout=15)
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap,
                                 auto_offset_reset="earliest", enable_auto_commit=False,
                                 consumer_timeout_ms=15000,
                                 value_deserializer=lambda value: json.loads(value.decode("utf-8")))
        message = next(iter(consumer), None)
        assert message is not None and message.value == payload, "Python Kafka round trip failed"
        print("PASS: Python producer and consumer exchanged one JSON array.")

        schema = ArrayType(StructType([
            StructField("message", StringType()),
            StructField("event_timestamp", StringType()),
        ]))
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", bootstrap)
               .option("subscribe", topic).option("startingOffsets", "earliest").load())
        parsed = (raw.select(F.explode(F.from_json(F.col("value").cast("string"), schema)).alias("event"))
                  .select("event.*"))
        runtime = Path(project_root) / ".runtime"
        runtime.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="smoke-checkpoint-", dir=runtime) as checkpoint:
            query = (parsed.writeStream.format("memory").queryName(table)
                     .option("checkpointLocation", checkpoint)
                     .trigger(availableNow=True).start())
            if not query.awaitTermination(60):
                query.stop()
                raise TimeoutError("Spark Kafka check exceeded 60 seconds")
            rows = spark.table(table).collect()
            assert len(rows) == 1 and rows[0]["message"] == "environment-ready", rows
            print("PASS: Spark Structured Streaming parsed the same record using the Kafka connector.")
        return {"python_round_trip": "PASS", "spark_streaming": "PASS", "records": len(rows)}
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.catalog.dropTempView(table)
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.close(timeout=10)
        if created:
            admin.delete_topics([topic])
        admin.close()
