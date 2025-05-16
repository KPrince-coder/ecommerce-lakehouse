# Bronze Layer ETL

This directory contains ETL processes for the bronze layer of the e-commerce lakehouse architecture.

## Overview

The bronze layer is the first layer of the lakehouse architecture. It ingests raw data from the raw layer and applies minimal transformations to create Delta tables. The main goals of the bronze layer are:

- Ingest raw data from various sources
- Apply schema to raw data
- Add metadata columns
- Partition data appropriately
- Write data to Delta tables
- Register tables in the Glue Data Catalog

## ETL Processes

The bronze layer includes the following ETL processes:

- **Products ETL**: Ingests product data from CSV files
- **Orders ETL**: Ingests order data from CSV files
- **Order Items ETL**: Ingests order item data from CSV files

## Data Quality

The bronze layer applies minimal data quality checks:

- Schema validation
- Basic data type validation
- Null checks for required fields

## Partitioning

The bronze layer partitions data as follows:

- Products: No partitioning
- Orders: Partitioned by date
- Order Items: Partitioned by date

## Metadata

The bronze layer adds the following metadata columns to all tables:

- `source_file`: The source file path
- `ingestion_timestamp`: The timestamp when the data was ingested
- `processing_timestamp`: The timestamp when the data was processed
- `layer`: The data layer (bronze)

## File Processing Status

The bronze layer tracks the status of files as they are processed:

- `raw/processing/`: Files currently being processed
- `raw/processed/`: Files successfully processed
- `raw/failed/`: Files that failed processing
- `raw/archive/`: Original files after successful processing

## Usage

Each ETL process can be run individually or as part of the overall ETL pipeline. For example:

```bash
# Run the products ETL
python -m etl.bronze.products_etl

# Run the orders ETL
python -m etl.bronze.orders_etl

# Run the order items ETL
python -m etl.bronze.order_items_etl
```
