# Dataset

This assessment uses the HI-Small subset of [IBM Transactions for Anti-Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml), as approved for Assessment 1.

## Download the files

Download and extract the HI-Small files from Kaggle, or copy the original files used in Assessment 1. Keep their contents unchanged.

Place the transaction file at `data/HI-Small_Trans.csv`. It contains the transaction details and laundering labels.

Place the account file at `data/HI-Small_accounts.csv`. It contains the bank, account and entity details.

## Check the files

The SHA-256 checksums recorded in Assessment 1 are:

```text
b19d39f515523373f991b689c07e11e7b0b95c17a2c27a87d91584ae16c5b040  data/HI-Small_Trans.csv
786808526e33cfc441212dd6fccda7edfc24172149bed59c6ef59b186836b014  data/HI-Small_accounts.csv
```

Run this from the project folder on macOS:

```bash
shasum -a 256 data/HI-Small_Trans.csv data/HI-Small_accounts.csv
```

Matching checksums confirm that the files are the same as those used in Assessment 1.

## Generated data

Run sections A1 and A2 of `assessment2.ipynb` to check the inputs and create the split.

`data/development_data.parquet/` contains development records with their training, validation and test assignments.

`data/stream_data.parquet/` contains the transactions reserved for the Kafka producer in Part B.

`data/split_manifest.json` records the input checksums, split settings, row counts and verification results.

The split targets 75% development and 25% streaming. A hash calculation includes seed 42 as a salt and keeps transactions with identical details in the same group, without using their labels. Seed 43 divides development into approximately 70% training, 15% validation and 15% test data. The actual counts are shown in the notebook and manifest.

`source_record_id` is each transaction's position in the original CSV, excluding the header. Duplicate rows have separate IDs but share a `transaction_group_id` and partition assignment. These IDs and the partition columns are used to check the split and are excluded from model features. All source rows are kept.

Each Parquet output is a folder containing Spark part files. Running the save cell again replaces these generated files. Keep the original CSV contents and row order to reproduce the same source IDs.
