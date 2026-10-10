# Assessment 2: Machine Learning and Real Time Streaming

Student ID: 33602190  
Unit: ITO5202  
Year: 2026

## Overview

This assessment uses the IBM AML HI-Small dataset approved for Assessment 1. Part A compares Spark ML classification models and saves the selected model. Part B uses that model to predict transactions sent through Kafka.

## Environment

The project runs on macOS Apple Silicon with Python 3.11.17, Java 17.0.20.1, PySpark 3.5.7 and Kafka 3.9.1. The Python Kafka library is `kafka-python3` 3.0.0, imported as `kafka3`.

XGBoost 3.0.5 uses its Spark classifier interface, with PyArrow 18.1.0 and scikit-learn 1.7.2 as dependencies. On Apple Silicon it also needs the OpenMP library, installed by setup through Homebrew. This project uses the CPU; no GPU is required.

Kafka runs locally in KRaft mode at `127.0.0.1:9092`. Its controller uses port 9093. This setup does not need Docker or ZooKeeper.

Spark uses the connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`, which matches its Scala 2.12 build. The Kafka server uses its own Scala 2.13 build. An internet connection is needed when Spark first downloads the connector.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run this from the project folder:

```bash
bash scripts/setup.sh
```

The script creates `.venv`, installs the Python packages and OpenMP, downloads and checks Java and Kafka, and registers the Python (assessment2-aml) kernel. Direct dependencies are listed in `requirements.txt`. The full package list is saved in `requirements-lock.txt`.

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

Open `assessment2.ipynb`, select Python (assessment2-aml) and run the cells in order. The setup displays the package versions and checks a sample Kafka message read by both Python and Spark. The temporary Kafka topic is removed after the check.

Kafka must be running for this check. Set `RUN_KAFKA_SMOKE_TEST = False` when working offline on Part A. Use the final notebook cell to stop Spark. Stop Kafka and Jupyter with Ctrl+C in their terminals. Kafka stores its local files under `.runtime/`.

To run the notebook from a terminal while Kafka is running:

```bash
.venv/bin/jupyter nbconvert --execute --to notebook --inplace \
  --ExecutePreprocessor.timeout=1800 assessment2.ipynb
```

## Data

The source is the HI-Small subset of [IBM Transactions for Anti-Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

Use the same `HI-Small_Trans.csv` and `HI-Small_accounts.csv` files approved for Assessment 1. The download instructions, file locations and checksums are in [data/README.md](data/README.md).

Sections A1 and A2 check the files, split the transactions into development and streaming data, and save both as Parquet. The split settings and results are recorded in `data/split_manifest.json`.

Section A3 adapts the sender entity and payment summaries from Assessment 1 using only the training records. It includes the classification baseline, bar charts, box plots, histograms and heatmaps to explore distributions and relationships. Relative amounts use each currency’s own percentiles so all 15 currencies can be explored without mixing monetary units. Account feature analysis, numeric correlations and feature selection also sit in A3, before the modelling tables.

Section A3 uses absolute Pearson correlation above 0.80 to identify overlapping numeric inputs. It records six removals and their correlations: sender_incoming_prior_count, sender_out_degree, sender_prior_count, receiver_outgoing_prior_count, receiver_in_degree and same_bank. These calculations remain in A3 so the selection can be reproduced. The heatmap shows the 28 retained numeric inputs.

Section A4 prepares one list of 34 inputs, including eight missing history indicators. These cover transaction details, previous and recent account activity, reverse payments, self transfers and bank categories. The recent distinct recipient count, recent distinct sender count and receiver outgoing count over 1 hour also remain excluded.

One account history workflow supplies all features. Accounts use bank and account together as their identifier. History includes only strictly earlier training transactions, excluding the current transaction and timestamp ties. Amount averages and flow ratios compare the same currency within 1 hour or 24 hours. Separate flags distinguish missing references from observed zeros.

The preprocessing pipeline keeps the 20 most frequent training banks in each role and groups the rest as Other. Categories are learned from training data and encoded consistently across all three partitions. The 28 numeric inputs and six categories produce a verified vector of 120 values. The feature tables and fitted preprocessing model are saved under `data/`.

Section A5 compares logistic regression, decision tree, random forest and XGBoost using the same 34 inputs and all training rows. Each model has two settings; validation F1 selects the settings, threshold and final model. A separate XGBoost comparison tests 32 inputs without bank categories against all 34 inputs at the same model settings.

XGBoost has the highest validation F1 at 0.5444, using no extra positive class weighting and a threshold of 0.2882150709629059. On the repeated test benchmark, precision is 74.92%, recall is 39.63% and F1 is 0.5184. The model detects 233 of 588 positive cases, misses 355 and produces 78 false alerts.

Removing bank categories gives validation F1 of 0.5435, so their additional contribution is small. Including them raises validation precision from 68.70% to 75.62% while lowering recall from 44.97% to 42.53% at the selected thresholds.

Section A6 saves the fitted preprocessing and XGBoost model to `models/a2_model`, with the selected threshold in `models/a2_settings.json`. The `predict_transactions` function applies this threshold to the model score.

To check the saved model without retraining, run the notebook setup followed by A6's Reload and predict cells. All 200 sample scores and predictions match after loading in a fresh session. A6 also saves the training transactions without labels to `data/training_history.parquet` for Part B's account features.

Earlier test results were reviewed before expanding the features, so the repeated test comparison is a benchmark rather than a fully independent confirmation. Streaming records remain reserved for Part B.

## References

[Assessment 1 notebook, sections 3.1 and 3.2](https://github.com/tung-lechu/assessment1-aml/blob/94acdaa21b071efc135a1fb4257710b8664f3fd0/assessment1.ipynb)

[IBM Transactions for Anti-Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

[XGBoost 3.0.5 Spark integration](https://xgboost.readthedocs.io/en/release_3.0.0/tutorials/spark_estimator.html)

[Spark 3.5.7 historical window boundaries](https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rangeBetween.html)

[Spark 3.5.7 precision recall evaluation](https://github.com/apache/spark/blob/v3.5.7/mllib/src/main/scala/org/apache/spark/mllib/evaluation/BinaryClassificationMetrics.scala)

[Apache Kafka 3.9 KRaft setup](https://kafka.apache.org/39/getting-started/quickstart/)

[Spark 3.5.7 Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
