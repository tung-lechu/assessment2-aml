# Assessment 2: Machine Learning and Real-Time Streaming

Student ID: 33602190  
Unit: ITO5202  
Year: 2026

## Overview

This assessment uses the approved IBM Transactions for Anti-Money Laundering HI-Small dataset from Assessment 1. Part A will compare and save Spark ML models. Part B will apply the saved pipeline to held-out records replayed through Kafka.

## Environment

The local setup uses Python 3.11.17, Java 17.0.20.1, PySpark 3.5.7,
Kafka 3.9.1 in single-node KRaft mode, and `kafka-python3` 3.0.0 (imported as
`kafka3`). It runs on macOS Apple Silicon.
Kafka listens only on `127.0.0.1:9092`; its internal controller uses port 9093.
Docker and ZooKeeper are not needed for this setup.

Spark uses `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. The connector
matches Spark's Scala 2.12 build; the broker's Scala 2.13 build is independent.
The first Spark start needs internet access to download the connector.

## Install

From the repository root, with [uv](https://docs.astral.sh/uv/getting-started/installation/)
installed, run:

```bash
bash scripts/setup.sh
```

This creates `.venv`, installs the locked Python dependencies, downloads and
checks the Java/Kafka archives, and registers **Python (assessment2-aml)**.
`requirements.txt` lists direct dependencies; `requirements-lock.txt` records
the full installed Python environment.

The setup script targets macOS Apple Silicon. On another platform, install the
equivalent Java 17 build and adjust `JAVA_HOME` in `scripts/env.sh` and the
notebook setup cell before registering the kernel. Re-run setup after moving
the repository because the registered kernel contains absolute local paths.

## Open and run the notebook

Open two terminals in the repository root. In the first:

```bash
bash scripts/start_kafka.sh
```

In the second:

```bash
bash scripts/start_notebook.sh
```

Open `assessment2.ipynb`, select **Python (assessment2-aml)** and run from the
top. The setup checks the versions, a small Spark computation, a Python Kafka
round trip and a Spark Structured Streaming micro-batch. It uses one synthetic
JSON array and removes its temporary Kafka topic afterward.

Kafka must be running for the connectivity check. When working offline on
Part A, set `RUN_KAFKA_SMOKE_TEST = False` to explicitly skip that check.
Stop Spark using the final notebook cell, and stop Kafka/Jupyter with Ctrl+C
in their respective terminals. Broker data is retained under `.runtime/`.

To execute the notebook from the terminal while Kafka is running:

```bash
.venv/bin/jupyter nbconvert --execute --to notebook --inplace \
  --ExecutePreprocessor.timeout=180 assessment2.ipynb
```

## Data

Source: [IBM Transactions for Anti-Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml), HI-Small subset.

Use the same `HI-Small_Trans.csv` and `HI-Small_accounts.csv` files approved for
Assessment 1. See [data/README.md](data/README.md) for download instructions,
file locations and checksums.

## References

- [IBM Transactions for Anti-Money Laundering (AML) — dataset source](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
- [Apache Kafka 3.9 local KRaft quick start](https://kafka.apache.org/39/getting-started/quickstart/)
- [Spark 3.5.7 Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
