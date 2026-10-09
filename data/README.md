# Dataset

Source: [IBM Transactions for Anti-Money Laundering (AML) on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml).

Assessment 2 reuses the HI-Small subset approved for Assessment 1.

## Download and file locations

Download and extract the following files from the source above, or copy the
original files used in Assessment 1. Place them in this repository's `data/`
folder without modifying their contents:

| File | Contents | Location |
| --- | --- | --- |
| `HI-Small_Trans.csv` | Transaction records and laundering labels | `data/HI-Small_Trans.csv` |
| `HI-Small_accounts.csv` | Account, entity and bank information | `data/HI-Small_accounts.csv` |

## File verification

The SHA-256 checksums recorded for the Assessment 1 files are:

```text
b19d39f515523373f991b689c07e11e7b0b95c17a2c27a87d91584ae16c5b040  data/HI-Small_Trans.csv
786808526e33cfc441212dd6fccda7edfc24172149bed59c6ef59b186836b014  data/HI-Small_accounts.csv
```

From the repository root on macOS, run:

```bash
shasum -a 256 data/HI-Small_Trans.csv data/HI-Small_accounts.csv
```

Compare the output with the values above to confirm that the file contents
match those used in Assessment 1.
