# City Transport ETL Project

This repository contains a PySpark ETL pipeline for processing city transport CSV data. It reads raw CSV files, applies transformations and aggregations, and writes clean data to Parquet, Hive, or Delta Lake. This project demonstrates a production-ready ETL workflow suitable for portfolio showcase.

## Features
- Extract: Reads CSV files containing transport station data.
- Transform: Renames and standardizes columns, cleans and validates data, aggregates passenger counts.
- Load: Writes processed data to Parquet, Hive, or Delta Lake.
- Reusable & Scalable: Modular design allows extension to other datasets.

## Sample Column Mapping
| Original Column    | New Column Name       |
|-------------------|--------------------|
| transport_id       | Transport_ID       |
| station_name       | Station_Name       |
| location           | Area               |
| passenger_count    | Passenger_Count    |
| last_updated       | Updated_Date       |

## Usage
1. Install dependencies:
pip install -r requirements.txt

2. Run ETL:
python src/city_transport_etl.py

3. Output: Processed data will be written to the folder `city-transport-analysis`.
