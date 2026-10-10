"""Replay the saved streaming transactions through Kafka."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

import pyarrow.parquet as pq
from kafka3 import KafkaProducer
from kafka3.admin import KafkaAdminClient, NewTopic
from kafka3.errors import KafkaError, TopicAlreadyExistsError

STREAM_PATH = Path(__file__).resolve().parent / "data/stream_data.parquet"
MESSAGE_COLUMNS = [
    "source_record_id", "timestamp", "from_bank", "sender_account",
    "to_bank", "receiver_account", "amount_received", "receiving_currency",
    "amount_paid", "payment_currency", "payment_format",
]


def read_batches(batch_size):
    files = sorted(STREAM_PATH.glob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No streaming data at {STREAM_PATH}. Run A2 first.")
    for path in files:
        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=batch_size, columns=MESSAGE_COLUMNS
        ):
            records = batch.to_pylist()
            for record in records:
                record["timestamp"] = record["timestamp"].replace(tzinfo=timezone.utc).isoformat()
            yield records


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Use a positive integer.")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=positive_integer, default=1000)
    parser.add_argument("--max-batches", type=positive_integer, help="Omit to replay all records.")
    parser.add_argument("--topic", default="aml-transactions")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers, request_timeout_ms=10000)
    try:
        admin.create_topics([NewTopic(args.topic, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda records: json.dumps(records, allow_nan=False).encode("utf-8"),
        acks="all", retries=3, max_in_flight_requests_per_connection=1,
    )
    total_records = 0
    batch_number = 0
    try:
        batches = islice(read_batches(args.batch_size), args.max_batches)
        for batch_number, records in enumerate(batches, start=1):
            if batch_number > 1:
                time.sleep(5)
            published_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            for record in records:
                record["event_timestamp"] = published_at
            result = producer.send(args.topic, records).get(timeout=30)
            total_records += len(records)
            print(
                f"Batch {batch_number}: records={len(records)} timestamp={published_at} "
                f"partition={result.partition} offset={result.offset}",
                flush=True,
            )
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    print(f"Sent {total_records:,} records in {batch_number} batches to {args.topic}.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Producer stopped.")
    except (KafkaError, OSError, ValueError) as error:
        print(f"Producer failed: {error}", file=sys.stderr)
        sys.exit(1)
