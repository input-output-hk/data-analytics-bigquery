## BigQuery schema

In this folder you will find the schema for the tables and views of the Cardano on BigQuery project.

### Tables
In the [tables](./tables) folder you will find the schema of the BigQuery tables in `json` format. You can use the BigQuery command line tool [bq](https://cloud.google.com/bigquery/docs/bq-command-line-tool) to create the tables.
For example the command:
```shell
bq mk --schema ./tables/schema_version.json iog-data-analytics:cardano_mainnet.schema_version
```
would create the `schema_version` table.

### Views
The [views](./views) folder contains the SQL code for all the views used by the update and compare scripts in the db-sync Postgres database.
All the views reside in the `analytics` database schema. 