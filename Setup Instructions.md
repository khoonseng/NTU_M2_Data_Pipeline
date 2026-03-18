# 1. Set up conda environment
```bash
conda env create -f environment.yml
```


# 2. Configure GCP project ID
Go to london_bicycles_dbt/profiles.yml file to update your GCP project ID.

![alt text](./docs/images/set%20up%20gcp%20project.png)


# 3. Run DBT commands for cloud data warehouse using "--target cloud"
```bash
cd london_bicycles_dbt # navigate to dbt folder
dbt clean # perform a clean of target and dbt_packages
dbt deps --target cloud # install dependencies
dbt seed --full-refresh --target cloud # create dim_date table
dbt run --full-refresh --target cloud # create staging tables and star schema tables
dbt test --target cloud # perform dbt data quality validations
dbt docs generate --target cloud # generate dbt docs
dbt docs serve --target cloud # launch dbt docs
```


# 4. Set up local DuckDB data warehouse
## (a) Prepare configuration file to pull data from Google Cloud Storage (GCS)
In the project root folder, create a ".env" file. Request owner to share service account key.

```
s3_access_key_id="<request for access key>"
s3_secret_access_key="<request for access key>"
```

## (b) Creation of local DuckDB database file
Navigate to scripts folder and execute python file to create db file
run these 3 commands in terminal:

```bash
conda activate london-bikes-env
cd scripts
python ingest_data_from_gcs.py
```

## (c) Verify local DuckDB data warehouse
Database file should be created in data/warehouse folder
- launch DBGate
- create new connection. choose DuckDB and select the database file
- test connection and connect
- database will be loaded with 83 million rows and 800 rows respectively

## (d) Run DBT commands for local data warehouse using "--target local"
```bash
cd london_bicycles_dbt # navigate to dbt folder
dbt clean # perform a clean of target and dbt_packages
dbt deps --target local # install dependencies
dbt seed --full-refresh --target local # create dim_date table
dbt run --full-refresh --target local # create staging tables and star schema tables
dbt test --target local # perform dbt data quality validations
dbt docs generate --target local # generate dbt docs
dbt docs serve --target local # launch dbt docs
```