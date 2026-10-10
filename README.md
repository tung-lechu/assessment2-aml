# Assessment 2: Machine Learning and Real Time Streaming

Student ID: 33602190  
Unit: ITO5202  
Year: 2026

## Overview

This assessment uses the IBM AML HI-Small dataset approved for Assessment 1. Part A compares Spark ML classification models and saves the selected model. Part B uses that model to predict transactions sent through Kafka.

## Environment

The project runs on macOS Apple Silicon with Python 3.11.17, Java 17.0.20.1, PySpark 3.5.7 and Kafka 3.9.1. The Python Kafka library is `kafka-python3` 3.0.0, imported as `kafka3`.

Kafka runs locally in KRaft mode at `127.0.0.1:9092`. Its controller uses port 9093. This setup does not need Docker or ZooKeeper.

Spark uses the connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`, which matches its Scala 2.12 build. The Kafka server uses its own Scala 2.13 build. An internet connection is needed when Spark first downloads the connector.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run this from the project folder:

```bash
bash scripts/setup.sh
```

The script creates `.venv`, installs the Python packages, downloads and checks Java and Kafka, and registers the Python (assessment2-aml) kernel. Direct dependencies are listed in `requirements.txt`. The full package list is saved in `requirements-lock.txt`.

On another operating system, install the appropriate Java 17 build and update `JAVA_HOME` in `scripts/env.sh` and the notebook setup cell before registering the kernel. Run setup again if the project folder moves, as the kernel stores the full local paths.

## Open the notebook

Open two terminals in the project folder. Start Kafka in the first:

```bash
bash scripts/start_kafka.sh
```

Start Jupyter in the second:

```bash
bash scripts/start_notebook.sh
```

Open `assessment2.ipynb`, select Python (assessment2-aml) and run the cells in order. The setup checks the package versions, a small Spark calculation and a sample Kafka message read by both Python and Spark. The temporary Kafka topic is removed after the check.

Kafka must be running for this check. Set `RUN_KAFKA_SMOKE_TEST = False` when working offline on Part A. Use the final notebook cell to stop Spark. Stop Kafka and Jupyter with Ctrl+C in their terminals. Kafka stores its local files under `.runtime/`.

To run the notebook from a terminal while Kafka is running:

```bash
.venv/bin/jupyter nbconvert --execute --to notebook --inplace \
  --ExecutePreprocessor.timeout=180 assessment2.ipynb
```

## Data

The source is the HI-Small subset of [IBM Transactions for Anti-Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

Use the same `HI-Small_Trans.csv` and `HI-Small_accounts.csv` files approved for Assessment 1. The download instructions, file locations and checksums are in [data/README.md](data/README.md).

Sections A1 and A2 check the files, split the transactions into development and streaming data, and save both as Parquet. The split settings and results are recorded in `data/split_manifest.json`.

Section A3 adapts the sender entity and payment summaries from Assessment 1 using only the training records. It adds the classification baseline, bar charts, box plots, histograms and heatmaps to explore distributions and relationships in the training data. Relative amounts use each currency’s own percentiles so all 15 currencies can be explored without mixing monetary units.

## References

[Assessment 1 notebook, sections 3.1 and 3.2](https://github.com/tung-lechu/assessment1-aml/blob/94acdaa21b071efc135a1fb4257710b8664f3fd0/assessment1.ipynb)

[IBM Transactions for Anti-Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

[Kaggle analysis used as a reference for the exploratory presentation](https://www.kaggle.com/code/ardabaranbaytar/anti-money-laundering-transaction-analysis-and-ris)

[Apache Kafka 3.9 KRaft setup](https://kafka.apache.org/39/getting-started/quickstart/)

[Spark 3.5.7 Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
