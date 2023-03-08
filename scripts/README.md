## Scripts
This folder contains the various scripts that are used in the BigQuery project. There are update, validation and export scripts. 
Most of the secrets/config that are used in the scripts are passed through environment variables in `json` format. 

### Update scripts
In this folder you will find all the scripts used for syncing (or "updating") of the data between db-sync and BigQuery.
Most of the scripts are bash scripts.
The scripts can be split into 3 categories:
- The scripts that update a table at the slot level. All the scripts with the naming convention `update_<table_name>.sh` fall into this category.
- The scripts that update a table at the epoch level. Certain tables need to be updated only every epoch. One example would be the rewards table, as we have new rewards only every new epoch. The scripts in this category have the naming conventions of `update_epoch_<table_name>.sh`.
- The aggregate scripts: these are the ones that call the scripts of the previous 2 categories: for example to run the update after an epoch boundary for all the tables. The scripts `run_bq_update.sh` and `run_update_epoch.sh` fall into this category.

### Validation scripts
The validation scripts can be found in the [deep_compare](./deep_compare) folder. These scripts validate that the data in BigQuery have a one to one correspondence with the data in db-sync.
They perform a deep comparison of the data using hashing. 

### Export scripts
The export scripts follow the naming pattern: `export_<table_name>.sh`. They are essentially backfill scripts used to populate in bulk the BigQuery tables. 
They are usually updating a table with all the data until a recent epoch.