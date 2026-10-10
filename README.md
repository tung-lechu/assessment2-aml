# Assessment 2: Machine Learning and Real Time Streaming

Student ID: 33602190  
Unit: ITO5202  
Year: 2026

## Overview

This project continues the analysis of the IBM AML HI Small dataset from Assessment 1. Part A compares four classification models and saves XGBoost for use in Part B. Part B sends the reserved transactions through Kafka, applies the saved model and counts predicted laundering cases in sliding windows.

## Environment

The project was run on macOS Apple Silicon with Python 3.11.17, Java 17.0.20.1, PySpark 3.5.7 and Kafka 3.9.1. It uses `kafka-python3` 3.0.0, imported as `kafka3`.

XGBoost 3.0.5 runs through its Spark interface. It needs PyArrow 18.1.0, `scikit-learn` 1.7.2 and OpenMP. The setup script installs OpenMP through Homebrew. Model training and prediction use the CPU.

Kafka runs in KRaft mode at `127.0.0.1:9092`, with its controller on port 9093. ZooKeeper and Docker are not needed for this setup.

The Spark connector is `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`. It matches Spark's Scala 2.12 build. Kafka itself uses Scala 2.13. Spark needs an internet connection to download the connector on its first run.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run this from the project folder:

```bash
bash scripts/setup.sh
```

The script creates `.venv`, installs the packages, downloads Java and Kafka, and registers the `Python (assessment2-aml)` notebook kernel. Direct dependencies are in `requirements.txt`; the full package list is in `requirements-lock.txt`.

For another operating system, install a suitable Java 17 build and update `JAVA_HOME` in `scripts/env.sh` and the notebook setup cell. Register the kernel after making those changes. Run setup again if the project folder moves, because the kernel uses absolute paths.

## Open the notebook

Open two terminals in the project folder. Start Kafka in the first:

```bash
bash scripts/start_kafka.sh
```

Start Jupyter in the second:

```bash
bash scripts/start_notebook.sh
```

Open `assessment2.ipynb`, select `Python (assessment2-aml)` and follow the cells in order. The setup checks the package versions and sends a sample Kafka message through Python and Spark. It removes the test topic afterwards.

Keep Kafka running for that check. When working offline on Part A, set `RUN_KAFKA_SMOKE_TEST = False`. The final notebook cell stops Spark. Press Ctrl+C in the Kafka and Jupyter terminals to stop those services. Kafka's local files are under `.runtime/`.

To run the notebook from a terminal while Kafka is running:

```bash
.venv/bin/jupyter nbconvert --execute --to notebook --inplace \
  --ExecutePreprocessor.timeout=1800 assessment2.ipynb
```

For another streaming run, choose new run names as described below. Reusing a topic and sending the same data again creates duplicate arrivals.

## Kafka producer

Run A1 and A2 first to create `data/stream_data.parquet`. The producer reads this saved subset and sends each batch as one JSON array. It creates the topic if it does not already exist.

Start Kafka in one terminal:

```bash
bash scripts/start_kafka.sh
```

In another terminal, send three batches of 1,000 records:

```bash
.venv/bin/python producer.py --batch-size 1000 --max-batches 3
```

To send the complete subset on a new topic:

```bash
.venv/bin/python producer.py --batch-size 50000 --topic aml-full-demo
```

Use the same topic in the consumer. B4 starts its own producer, so this manual command is only needed when running the producer separately.

The defaults are 1,000 records per batch, topic `aml-transactions` and server `127.0.0.1:9092`. Change these with `--batch-size`, `--topic` and `--bootstrap-servers`. Leave out `--max-batches` to send every record. A batch can be smaller when it reaches the end of a Parquet file.

The reserved subset has 1,269,694 records. At 1,000 records per batch, it produces 1,280 messages and about 107 minutes of pauses. At 50,000, it produces 29 messages and 2.3 minutes of pauses. Sending and processing take additional time. New topics allow messages up to 32 MiB. Use a new topic for larger batches, as existing topics keep their previous limit.

Each record includes its source ID, original transaction timestamp and a separate UTC `event_timestamp` for publication time. The producer leaves out labels and split columns. It waits for Kafka to acknowledge each message, logs the count, timestamp and offset, then pauses for five seconds before sending the next batch. Press Ctrl+C to stop the producer or Kafka in its terminal.

B1 checks three batches on a separate topic and compares the received data with the source. All 3,000 records matched. B4 covers the complete replay and model predictions.

## Streaming predictions

B2 prepares the 34 model inputs and applies the saved XGBoost pipeline and threshold. Account features use earlier training transactions as their history. Incoming records do not update that history, and the consumer does not retrain the model.

To run B2 in a fresh session after completing Part A:

1. Start Kafka with `bash scripts/start_kafka.sh` and open the notebook.
2. Run the environment and Start Spark cells.
3. Run A3's six function cells: `account_events`, `account_activity`, `counterparty_activity`, `pair_activity`, `combine_account_features` and `prepare_features`.
4. Run B1's message schema cell.
5. Run B2 in order. It loads the model, starts the consumer, sends three batches, checks the predictions and stops both queries.

The default run name is `b2_demo`, which uses topic `aml-b2-demo`. Predictions and their inputs are saved under `data/stream_predictions/b2_demo/`. The checkpoints are under `checkpoints/b2_demo/predictions/` and `checkpoints/b2_demo/console/`.

The consumer checks for data every five seconds and reads at most one Kafka message per trigger. With the default producer settings, that message contains up to 1,000 transactions. Feature preparation and prediction run inside `foreachBatch`. Each Spark batch is saved in its own Parquet folder. If a batch is retried, an already completed folder is kept. A separate query reads the saved files and prints five predictions at a time.

For a new demonstration, change `STREAM_RUN` before starting. To resume an existing run, keep its name, topic, output and checkpoints, and skip the producer cell. Keep each checkpoint with its original topic, model and output folder.

To send records manually while the B2 consumer is running:

```bash
.venv/bin/python producer.py --topic aml-b2-demo --batch-size 1000 --max-batches 3
```

Invalid messages stop the prediction query and report their Kafka offsets. Use `prediction_query.exception()` and `prediction_query.recentProgress` to investigate. B2's final cell stops its queries. Run Stop Spark when finished, then press Ctrl+C in the Kafka terminal.

The B2 check saved 3,000 records. All source values, model inputs, scores and predictions matched batch processing. Restarting from the checkpoint left the count at 3,000. Processing took 8.8 to 9.2 seconds per batch, longer than the five second producer pause. B4 uses larger batches to reduce the total replay time.

## Window monitoring

B3 counts positive predictions in 10 minute windows, with a new window every 2 minutes. It uses `event_timestamp`, a 1 minute watermark and a five second trigger. Each transaction appears in five overlapping windows, so adding the window counts would count transactions more than once.

Run B3 after B2 has saved predictions. In a fresh session, run the environment and Start Spark cells first, then set `STREAM_RUN` to the B2 run being monitored. B3 reads the saved Parquet files, so it does not need to repeat Kafka ingestion or model prediction.

The console query uses update mode to show counts while windows are still open. Its default checkpoint is `checkpoints/b2_demo/windows_console/`. Keep it when resuming the same query. Use a new checkpoint if the source, window settings or watermark change. The totals cell stops the query after processing the available files.

A separate timing test checks events arriving out of order and events whose windows have expired. Its files are under `.runtime/window_checks/`. The test stops its query when finished.

The watermark advances when newer event timestamps arrive. Waiting for one minute without new events does not close the remaining windows. The allowance covers twelve planned producer intervals. A queue of events still arriving in order delays the output without necessarily causing the watermark to reject them.

B2's 3,000 records produced five open windows with zero positive predictions. All counts matched the batch calculation. The separate timing test also matched all seven expected windows. It included the event arriving 30 seconds behind and dropped the expired event from all five of its windows. Restarting the monitor did not read the records again.

## Complete streaming run

B4 checks recovery first, then runs the full reserved subset. It sends ten small batches to `aml-b4-check`, stops the consumer after its first saved batch and restarts it from the same checkpoints. The full replay uses `aml-b4-full-50k`. Keep Kafka running throughout; B4 starts the producer itself.

For a fresh session, run the environment and Start Spark cells, the six A3 feature functions, B1's schema, B2's load and function cells, and B3's Read the predictions cell. Then run B4 in order. Complete B2 at least once first, because B4 uses its saved prediction schema.

The restart check uses 1,000 records per batch. The full replay uses 50,000 and keeps the five second pause. Both the producer and new topics have a 32 MiB message limit. The consumer uses the same fetch limit per partition and reads up to two queued messages per trigger. Predictions and window counts run together. Window updates are saved, with five rows printed every twelve update batches.

The full replay produces 29 messages. Its 28 pauses take 2.3 minutes, compared with 106.6 minutes at 1,000 records per batch. Predictions are saved under `data/stream_predictions/b4_full_50k/`, window updates under `data/stream_windows/b4_full_50k/`, and checkpoints under `checkpoints/b4_full_50k/`. Logs and batch timings are under `.runtime/b4_full_50k/`. These generated files stay local.

For another full replay, choose unused `SHORT_RUN` and `FULL_RUN` names. To resume only the consumer after all records have been sent, keep the full run name and load the same definitions. Run `start_run(FULL_RUN)`, then call `processAllAvailable()` on the prediction query followed by the window query. Skip the producer start to avoid sending the records again.

B4 stops its queries after processing the available records. Run Stop Spark at the end of the notebook and press Ctrl+C in the Kafka terminal. If a run is interrupted, stop its producer and active queries before starting another.

The complete run took 4 minutes 34 seconds. It saved all 1,269,694 records once, including 778 positive predictions. Every source field and all six window counts matched, with no window memberships dropped. The restart check recovered 9,000 queued records without duplicating the first 1,000.

Median processing time was 17.57 seconds per Spark batch. The median delay for the oldest event was 55.62 seconds, rising to 88.70 seconds as the queue grew. The notebook includes the timing plot and a reflection on model choice, windows, implementation challenges and capacity.

## Data and analysis

The data comes from the HI Small subset of [IBM Transactions for Anti Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

Use the same `HI-Small_Trans.csv` and `HI-Small_accounts.csv` files approved for Assessment 1. Download instructions, file locations and checksums are in [data/README.md](data/README.md).

A1 and A2 check the source files, split the transactions and save the development and streaming subsets as Parquet. The split settings and counts are in `data/split_manifest.json`.

A3 reuses the sender entity and payment summaries from Assessment 1, recalculated using training records only. It explores payment formats, currencies, timing and account behaviour through bar charts, box plots, histograms and heatmaps. Amount percentiles are calculated within each currency so the analysis does not mix monetary units.

Feature analysis and correlation checks appear before A4. Absolute Pearson correlation above 0.80 removes six inputs: `sender_incoming_prior_count`, `sender_out_degree`, `sender_prior_count`, `receiver_outgoing_prior_count`, `receiver_in_degree` and `same_bank`. Their calculations and removal reasons remain in A3. The final heatmap covers the 28 retained numeric inputs.

A4 uses 34 inputs, including eight indicators for missing history. The inputs cover transaction details, account activity, reverse payments, self transfers and bank categories. Recent distinct counterparty counts and the receiver outgoing count over 1 hour are excluded.

An account is identified by its bank and account number together. History includes only training transactions with an earlier timestamp. Transactions at the same timestamp are excluded. Amount averages and flow ratios use the same currency within 1 hour or 24 hours. Missing history has separate flags so it can be distinguished from an observed zero.

The pipeline keeps the 20 most frequent training banks in each role and groups the rest as Other. It learns categories from training data and applies the same encoding to validation and test data. The 28 numeric inputs and six categories produce 120 vector values. Feature tables and the fitted preprocessing model are saved under `data/`.

A5 compares logistic regression, decision tree, random forest and XGBoost with two settings per model. Validation F1 selects the model settings, threshold and final model. A separate XGBoost comparison checks whether adding bank categories improves the results.

XGBoost had the highest validation F1 at 0.5444, without extra positive class weighting. Its selected threshold is 0.2882150709629059. On the repeated test benchmark, precision was 74.92%, recall was 39.63% and F1 was 0.5184. It detected 233 of 588 positive cases, missed 355 and produced 78 false alerts.

Bank categories made a small difference. Removing them gave validation F1 of 0.5435. Including them increased precision from 68.70% to 75.62% but reduced recall from 44.97% to 42.53% at the respective thresholds.

A6 saves preprocessing and XGBoost together under `models/a2_model`, with the threshold in `models/a2_settings.json`. The `predict_transactions` function applies that threshold to the model score.

To check the saved model, run the notebook setup and A6's Reload and predict cells. All 200 sample scores and predictions matched after loading in a fresh session. A6 also saves training transactions without labels to `data/training_history.parquet` for the streaming features.

Earlier test results were reviewed before the features were expanded. The repeated test results therefore serve as a benchmark, while the reserved streaming subset is used only in Part B.

## References

[Assessment 1 notebook, sections 3.1 and 3.2](https://github.com/tung-lechu/assessment1-aml/blob/94acdaa21b071efc135a1fb4257710b8664f3fd0/assessment1.ipynb)

[IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

[XGBoost 3.0.5 Spark integration](https://xgboost.readthedocs.io/en/release_3.0.0/tutorials/spark_estimator.html)

[Spark 3.5.7 historical window boundaries](https://spark.apache.org/docs/3.5.7/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rangeBetween.html)

[Spark 3.5.7 precision recall evaluation](https://github.com/apache/spark/blob/v3.5.7/mllib/src/main/scala/org/apache/spark/mllib/evaluation/BinaryClassificationMetrics.scala)

[Apache Kafka 3.9 KRaft setup](https://kafka.apache.org/39/getting-started/quickstart/)

[Spark 3.5.7 Kafka integration](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html)
