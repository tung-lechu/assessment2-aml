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

Section A3 creates and analyses the account history used by all models. Its intermediate tables are:

| Table | Contents |
|---|---|
| `data/account_activity.parquet/` | Previous and recent incoming and outgoing counts, gaps and amounts |
| `data/counterparty_activity.parquet/` | Historical distinct counterparties |
| `data/pair_activity.parquet/` | Previous payments between each pair and earlier reverse payments |
| `data/feature_history.parquet/` | Combined transaction and account features before selection |

Accounts use bank and account together as their identifier. History includes only strictly earlier training transactions, excluding timestamp ties. Validation and test transactions do not update it. Amount averages and ratios use the same currency, with 1 hour and 24 hour windows. Self transfer requires matching bank and account; reverse payments exclude self transfers.

A3 identifies overlapping numeric inputs using absolute Pearson correlation above 0.80. It retains the calculations and a removal table for sender_incoming_prior_count, sender_out_degree, sender_prior_count, receiver_outgoing_prior_count, receiver_in_degree and same_bank. These six fields are excluded from A4, along with the previously removed sender_unique_receivers_24h, receiver_unique_senders_24h and receiver_outgoing_count_1h.

A4 prepares one list of 34 inputs, including eight missing history indicators. The three saved tables are `data/model_training.parquet/`, `data/model_validation.parquet/` and `data/model_test.parquet/`. The revised schema has 36 columns: the same 34 inputs, the target and the source record ID. Missing gaps and ratios use zero placeholders with separate flags. All retained feature values, IDs and labels match the previous tables across the 3,808,651 development transactions.

`data/preprocessing_model/` contains the fitted Spark preprocessing stages. The pipeline retains the 20 most frequent training banks in each role and groups other or unseen banks as Other. Category encoding is learned from training data. The 28 numeric inputs and six categories produce a verified vector of 120 values. The same fitted stages transform all three tables.

The four A5 classifiers use these same 34 inputs and the 120 value vector. A separate XGBoost comparison excludes the two bank categories, leaving 32 inputs. The A5 rerun loads the saved tables and fitted preprocessing model without changing them.

A6 saves `data/training_history.parquet/` with the 2,664,035 training transactions and no labels. Part B will use this fixed reference with the same time cutoffs as A3. Validation, test and streaming records do not update it.

The A6 reload check uses `data/model_reload_input.parquet/` and `data/model_reload_expected.parquet/`. These contain 200 prepared records without labels and their original scores and predictions. Run A6's save cells to recreate them.

The final pipeline is saved under `models/a2_model/`. Its threshold and feature list are in `models/a2_settings.json`. Use A6's `predict_transactions` function to apply the saved threshold.

The split targets 75% development and 25% streaming. A hash calculation includes seed 42 as a salt and keeps transactions with identical details in the same group, without using their labels. Seed 43 divides development into approximately 70% training, 15% validation and 15% test data. The actual counts are shown in the notebook and manifest.

`source_record_id` is each transaction's position in the original CSV, excluding the header. Duplicate rows have separate IDs but share a `transaction_group_id` and partition assignment. These IDs and the partition columns are used to check the split and are excluded from model features. All source rows are kept.

Each Parquet output is a folder containing Spark part files. Running the save cell again replaces these generated files. Keep the original CSV contents and row order to reproduce the same source IDs.
